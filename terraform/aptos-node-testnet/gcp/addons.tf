locals {
  chaos_mesh_helm_chart_path = "${path.module}/../../helm/chaos"
}

resource "kubernetes_namespace" "chaos-mesh" {
  metadata {
    annotations = {
      name = "chaos-mesh"
    }

    name = "chaos-mesh"
  }
}

resource "helm_release" "chaos-mesh" {
  name      = "chaos-mesh"
  namespace = kubernetes_namespace.chaos-mesh.metadata[0].name

  chart       = local.chaos_mesh_helm_chart_path
  max_history = 5
  wait        = false

  values = [
    jsonencode({
      chaos-mesh = {
        chaosDaemon = {
          runtime    = "containerd"
          socketPath = "/run/containerd/containerd.sock"
          image = {
            repository = "aptos-internal/chaos-daemon"
            tag        = "latest"
          }
        },
        controllerManager = {
          image = {
            repository = "aptos-internal/chaos-mesh"
            tag        = "latest"
          }
        },
        dashboard = {
          image = {
            repository = "aptos-internal/chaos-dashboard"
            tag        = "latest"
          }
        }
        images = {
          registry = "us-west1-docker.pkg.dev/aptos-global"
        }
      }
    })
  ]

  # inspired by https://stackoverflow.com/a/66501021 to trigger redeployment whenever any of the charts file contents change.
  set {
    name  = "chart_sha1"
    value = sha1(join("", [for f in fileset(local.chaos_mesh_helm_chart_path, "**") : filesha1("${local.chaos_mesh_helm_chart_path}/${f}")]))
  }
}

resource "google_service_account" "k8s-gcp-integrations" {
  project    = var.project
  account_id = "${local.workspace_name}-testnet-gcp"
}

resource "google_project_iam_member" "k8s-gcp-integrations-dns" {
  project = local.zone_project
  role    = "roles/dns.admin"
  member  = "serviceAccount:${google_service_account.k8s-gcp-integrations.email}"
}

resource "google_service_account_iam_binding" "k8s-gcp-integrations" {
  service_account_id = google_service_account.k8s-gcp-integrations.name
  role               = "roles/iam.workloadIdentityUser"
  members            = ["serviceAccount:${module.validator.gke_cluster_workload_identity_config[0].workload_pool}[kube-system/k8s-gcp-integrations]"]
}

resource "kubernetes_service_account" "k8s-gcp-integrations" {
  metadata {
    name      = "k8s-gcp-integrations"
    namespace = "kube-system"
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.k8s-gcp-integrations.email
    }
  }
}

data "google_dns_managed_zone" "testnet" {
  count   = var.zone_name != "" ? 1 : 0
  name    = var.zone_name
  project = local.zone_project
}

locals {
  zone_project = var.zone_project != "" ? var.zone_project : var.project
  dns_prefix   = var.workspace_dns ? "${local.workspace_name}.${var.dns_prefix_name}." : "${var.dns_prefix_name}."
  domain       = var.zone_name != "" ? trimsuffix("${local.dns_prefix}${data.google_dns_managed_zone.testnet[0].dns_name}", ".") : null
}

resource "helm_release" "external-dns" {
  count       = var.zone_name != "" ? 1 : 0
  name        = "external-dns"
  repository  = "https://kubernetes-sigs.github.io/external-dns"
  chart       = "external-dns"
  version     = "1.11.0"
  namespace   = "kube-system"
  max_history = 5
  wait        = false

  values = [
    jsonencode({
      serviceAccount = {
        create = false
        name   = kubernetes_service_account.k8s-gcp-integrations.metadata[0].name
      }
      provider      = "google"
      domainFilters = var.zone_name != "" ? [data.google_dns_managed_zone.testnet[0].dns_name] : []
      extraArgs = [
        "--google-project=${local.zone_project}",
        "--txt-owner-id=${local.workspace_name}",
        "--txt-prefix=aptos",
      ]
    })
  ]
}
