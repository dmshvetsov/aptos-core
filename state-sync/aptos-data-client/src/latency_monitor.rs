// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    client::AptosDataClient,
    interface::AptosDataClientInterface,
    logging::{LogEntry, LogEvent, LogSchema},
    metrics,
};
use aptos_config::config::AptosDataClientConfig;
use aptos_infallible::RwLock;
use aptos_logger::{info, sample, sample::SampleRate, warn};
use aptos_storage_interface::DbReader;
use aptos_time_service::{TimeService, TimeServiceTrait};
use futures::StreamExt;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

// Useful constants
const COMMIT_TO_SEEN_LATENCY_METRIC_NAME: &str = "commit_to_seen_latency";
const LATENCY_MONITOR_LOG_FREQ_SECS: u64 = 5;
const MAX_VERSION_LAG_TO_TOLERATE: u64 = 10000;
const MAX_NUM_TRACKED_VERSION_ENTRIES: usize = 10_000;
const SEEN_TO_SYNC_LATENCY_METRIC_NAME: &str = "seen_to_sync_latency";

/// A simple monitor that tracks the latencies taken by the data client
/// to: (i) see new data once it has been committed; and (ii) request
/// and synchronize that data locally (i.e., to the local storage).
#[derive(Clone)]
pub struct LatencyMonitor {
    advertised_version_timestamps: Arc<RwLock<BTreeMap<u64, Instant>>>, // The timestamps when advertised versions were first seen
    caught_up_to_latest: bool, // Whether the node has ever caught up to the latest state
    data_client: AptosDataClient, // The data client through which to see advertised data
    monitor_loop_interval: Duration, // The interval between latency monitor loop executions
    storage: Arc<dyn DbReader>, // The reader interface to storage
    time_service: TimeService, // The service to monitor elapsed time
}

impl LatencyMonitor {
    pub fn new(
        data_client_config: AptosDataClientConfig,
        data_client: AptosDataClient,
        storage: Arc<dyn DbReader>,
        time_service: TimeService,
    ) -> Self {
        let monitor_loop_interval =
            Duration::from_millis(data_client_config.latency_monitor_loop_interval_ms);

        Self {
            advertised_version_timestamps: Arc::new(RwLock::new(BTreeMap::new())),
            caught_up_to_latest: false,
            data_client,
            monitor_loop_interval,
            storage,
            time_service,
        }
    }

    /// Starts the latency monitor and periodically updates the latency metrics
    pub async fn start_latency_monitor(mut self) {
        info!(
            (LogSchema::new(LogEntry::LatencyMonitor)
                .message("Starting the Aptos data client latency monitor!"))
        );
        let loop_ticker = self.time_service.interval(self.monitor_loop_interval);
        futures::pin_mut!(loop_ticker);

        // Start the monitor
        loop {
            // Wait for the next round before updating the monitor
            loop_ticker.next().await;

            // Get the highest synced version from storage
            let highest_synced_version = match self.storage.get_latest_version() {
                Ok(version) => version,
                Err(error) => {
                    sample!(
                        SampleRate::Duration(Duration::from_secs(LATENCY_MONITOR_LOG_FREQ_SECS)),
                        warn!(
                            (LogSchema::new(LogEntry::LatencyMonitor)
                                .event(LogEvent::StorageReadFailed)
                                .message(&format!("Unable to read the highest synced version: {:?}", error)))
                        );
                    );
                    continue; // Continue to the next round
                },
            };

            // Update the latency metrics for all versions that we've now synced
            self.update_latency_metrics(highest_synced_version);

            // Get the highest advertised version from the global data summary
            let advertised_data = &self.data_client.get_global_data_summary().advertised_data;
            let highest_advertised_version = match advertised_data.highest_synced_ledger_info() {
                Some(ledger_info) => ledger_info.ledger_info().version(),
                None => {
                    sample!(
                        SampleRate::Duration(Duration::from_secs(LATENCY_MONITOR_LOG_FREQ_SECS)),
                        warn!(
                            (LogSchema::new(LogEntry::LatencyMonitor)
                                .event(LogEvent::AggregateSummary)
                                .message("Unable to get the highest advertised version!"))
                        );
                    );
                    continue; // Continue to the next round
                },
            };

            // Update the advertised version timestamps
            self.update_advertised_version_timestamps(
                highest_synced_version,
                highest_advertised_version,
            );
        }
    }

    /// Calculates the duration between the commit time and the current time.
    /// If the commit time is not in the past, this function returns None.
    ///
    /// Note: the commit timestamp is the duration (in microseconds) since the
    /// Unix epoch (when the block containing the transaction was proposed).
    fn calculate_duration_from_commit_to_seen(
        &self,
        commit_timestamp_usecs: u64,
    ) -> Option<Duration> {
        let now_timestamp_usecs = self.time_service.now_unix_time().as_micros() as u64;
        if now_timestamp_usecs > commit_timestamp_usecs {
            Some(Duration::from_micros(
                now_timestamp_usecs - commit_timestamp_usecs,
            ))
        } else {
            // Log the error and return None
            sample!(
                SampleRate::Duration(Duration::from_secs(LATENCY_MONITOR_LOG_FREQ_SECS)),
                warn!(
                    (LogSchema::new(LogEntry::LatencyMonitor)
                        .event(LogEvent::UnexpectedError)
                        .message("The commit timestamp appears to be in the future!"))
                );
            );
            None
        }
    }

    /// Updates the latency metrics for all versions that have now been synced
    fn update_latency_metrics(&self, highest_synced_version: u64) {
        // Split the advertised versions into synced and unsynced versions
        let unsynced_advertised_versions = self
            .advertised_version_timestamps
            .write()
            .split_off(&(highest_synced_version + 1));

        // Update the metrics for all synced versions
        for (synced_version, timestamp) in self.advertised_version_timestamps.read().iter() {
            // Update the seen to synced latencies
            let duration_from_seen_to_synced = self.time_service.now().duration_since(*timestamp);
            metrics::observe_value_with_label(
                &metrics::SYNC_LATENCIES,
                SEEN_TO_SYNC_LATENCY_METRIC_NAME,
                duration_from_seen_to_synced.as_secs_f64(),
            );

            // Update the commit to seen latencies
            if let Ok(block_timestamp) = self.storage.get_block_timestamp(*synced_version) {
                if let Some(duration_from_commit_to_seen) =
                    self.calculate_duration_from_commit_to_seen(block_timestamp)
                {
                    metrics::observe_value_with_label(
                        &metrics::SYNC_LATENCIES,
                        COMMIT_TO_SEEN_LATENCY_METRIC_NAME,
                        duration_from_commit_to_seen.as_secs_f64(),
                    );
                }
            }
        }

        // Update the advertised versions with those we still need to sync
        *self.advertised_version_timestamps.write() = unsynced_advertised_versions;
    }

    /// Updates the advertised version timestamps by inserting any newly seen versions
    /// into the map and garbage collecting any old versions.
    fn update_advertised_version_timestamps(
        &mut self,
        highest_synced_version: u64,
        highest_advertised_version: u64,
    ) {
        // Check if we're still catching up to the latest version
        if !self.caught_up_to_latest {
            if highest_synced_version >= highest_advertised_version - MAX_VERSION_LAG_TO_TOLERATE {
                info!(
                    (LogSchema::new(LogEntry::LatencyMonitor)
                        .event(LogEvent::CaughtUpToLatest)
                        .message(
                            "We've caught up to the latest version! Starting the latency monitor."
                        ))
                );
                self.caught_up_to_latest = true; // We've caught up
            } else {
                return; // We're still catching up, so we shouldn't update the advertised version timestamps
            }
        }

        // If we're already synced with the highest advertised version, there's nothing to do
        if highest_synced_version >= highest_advertised_version {
            return;
        }

        // Insert the newly seen version into the advertised version timestamps
        self.advertised_version_timestamps
            .write()
            .insert(highest_advertised_version, self.time_service.now());

        // If the map is too large, garbage collect the old versions
        while self.advertised_version_timestamps.read().len() > MAX_NUM_TRACKED_VERSION_ENTRIES {
            // Remove the lowest version from the map by popping the first
            // item in the map. This is possible because BTreeMaps are sorted.
            self.advertised_version_timestamps.write().pop_first();
        }
    }
}
