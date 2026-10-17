terraform {
  required_version = "~> 1.14.0"
  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "0.11.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "3.1.1"
    }
  }
}

variable "image" {
  description = "Image with the operator, the workers and the console"
  type        = string
  default     = "grantline:dev"
}

variable "replications" {
  description = "Replications the console shows"
  type        = list(string)
  default     = ["copy"]
}

resource "kind_cluster" "this" {
  name           = "grantline"
  node_image     = "kindest/node:v1.35.0"
  wait_for_ready = true

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"
    node {
      role = "control-plane"
    }
  }

  # A local image isn't in any registry the node can reach.
  provisioner "local-exec" {
    command = "kind load docker-image ${var.image} --name ${self.name}"
  }
}

provider "helm" {
  kubernetes = {
    host                   = kind_cluster.this.endpoint
    cluster_ca_certificate = kind_cluster.this.cluster_ca_certificate
    client_certificate     = kind_cluster.this.client_certificate
    client_key             = kind_cluster.this.client_key
  }
}

resource "helm_release" "grantline" {
  name             = "grantline"
  namespace        = "grantline"
  create_namespace = true
  chart            = "${path.module}/../helm/grantline"
  values = [yamlencode({
    image   = var.image
    console = { enabled = true, replications = var.replications }
  })]
}

output "kubeconfig" {
  value     = kind_cluster.this.kubeconfig
  sensitive = true
}
