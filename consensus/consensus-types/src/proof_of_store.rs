// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::common::Round;
use anyhow::{bail, Context};
use aptos_crypto::{bls12381, CryptoMaterialError, HashValue};
use aptos_crypto_derive::{BCSCryptoHash, CryptoHasher};
use aptos_types::{
    aggregate_signature::AggregateSignature, validator_signer::ValidatorSigner,
    validator_verifier::ValidatorVerifier, PeerId,
};
use rand::{seq::SliceRandom, thread_rng};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub struct LogicalTime {
    epoch: u64,
    round: Round,
}

impl LogicalTime {
    pub fn new(epoch: u64, round: Round) -> Self {
        Self { epoch, round }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn round(&self) -> Round {
        self.round
    }
}

#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, CryptoHasher, BCSCryptoHash,
)]
pub struct BatchId {
    pub id: u64,
    /// A random number that is stored in the DB and updated only if the value does not exist in
    /// the DB: (a) at the start of an epoch, or (b) the DB was wiped. When the nonce is updated,
    /// id starts again at 0.
    pub nonce: u64,
}

impl BatchId {
    pub fn new(nonce: u64) -> Self {
        Self { id: 0, nonce }
    }

    pub fn new_for_test(id: u64) -> Self {
        Self { id, nonce: 0 }
    }

    pub fn increment(&mut self) {
        self.id += 1;
    }
}

impl PartialOrd<Self> for BatchId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BatchId {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.id.cmp(&other.id) {
            Ordering::Equal => {},
            ordering => return ordering,
        }
        self.nonce.cmp(&other.nonce)
    }
}

impl Display for BatchId {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "({}, {})", self.id, self.nonce)
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, CryptoHasher, BCSCryptoHash, PartialEq, Eq, Hash,
)]
pub struct BatchInfo {
    pub author: PeerId,
    pub batch_id: BatchId,
    pub expiration: LogicalTime,
    pub digest: HashValue,
    pub num_txns: u64,
    pub num_bytes: u64,
}

impl BatchInfo {
    pub fn new(
        author: PeerId,
        batch_id: BatchId,
        expiration: LogicalTime,
        digest: HashValue,
        num_txns: u64,
        num_bytes: u64,
    ) -> Self {
        Self {
            author,
            batch_id,
            expiration,
            digest,
            num_txns,
            num_bytes,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.expiration.epoch
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SignedDigest {
    signer: PeerId,
    info: BatchInfo,
    signature: bls12381::Signature,
}

impl SignedDigest {
    pub fn new(
        batch_info: BatchInfo,
        validator_signer: &ValidatorSigner,
    ) -> Result<Self, CryptoMaterialError> {
        let signature = validator_signer.sign(&batch_info)?;

        Ok(Self {
            signer: validator_signer.author(),
            info: batch_info,
            signature,
        })
    }

    pub fn signer(&self) -> PeerId {
        self.signer
    }

    pub fn epoch(&self) -> u64 {
        self.info.epoch()
    }

    pub fn verify(&self, sender: PeerId, validator: &ValidatorVerifier) -> anyhow::Result<()> {
        if sender == self.signer {
            Ok(validator.verify(self.signer, &self.info, &self.signature)?)
        } else {
            bail!("Sender {} mismatch signer {}", sender, self.signer);
        }
    }

    pub fn info(&self) -> &BatchInfo {
        &self.info
    }

    pub fn signature(self) -> bls12381::Signature {
        self.signature
    }

    pub fn digest(&self) -> HashValue {
        self.info.digest
    }
}

#[derive(Debug, PartialEq)]
pub enum SignedDigestError {
    WrongAuthor,
    WrongInfo,
    DuplicatedSignature,
    InvalidAuthor,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct ProofOfStore {
    info: BatchInfo,
    multi_signature: AggregateSignature,
}

impl ProofOfStore {
    pub fn new(info: BatchInfo, multi_signature: AggregateSignature) -> Self {
        Self {
            info,
            multi_signature,
        }
    }

    pub fn info(&self) -> &BatchInfo {
        &self.info
    }

    pub fn digest(&self) -> &HashValue {
        &self.info.digest
    }

    pub fn expiration(&self) -> LogicalTime {
        self.info.expiration
    }

    pub fn verify(&self, validator: &ValidatorVerifier) -> anyhow::Result<()> {
        validator
            .verify_multi_signatures(&self.info, &self.multi_signature)
            .context("Failed to verify ProofOfStore")
    }

    pub fn shuffled_signers(&self, validator: &ValidatorVerifier) -> Vec<PeerId> {
        let mut ret: Vec<PeerId> = self
            .multi_signature
            .get_voter_addresses(&validator.get_ordered_account_addresses());
        ret.shuffle(&mut thread_rng());
        ret
    }

    pub fn epoch(&self) -> u64 {
        self.info.expiration.epoch
    }
}
