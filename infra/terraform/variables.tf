# ══════════════════════════════════════════════════════════════════
# infra/terraform/variables.tf — Variables de entrada para AWS
#
# JUSTIFICACIÓN DE REGIÓN (us-east-1 / N. Virginia):
#   1. Mayor disponibilidad de instancias t3.* On-Demand.
#   2. Región con mayor cantidad de AMIs y servicios disponibles.
#   3. Latencia intra-VPC < 0.3 ms, despreciable frente a las
#      latencias medidas por el benchmark (batch: segundos,
#      streaming: decenas de ms).
# ══════════════════════════════════════════════════════════════════

variable "aws_access_key" {
  description = "AWS Access Key ID. Obtener en: IAM → Security credentials → Access keys"
  type        = string
  sensitive   = true
}

variable "aws_secret_key" {
  description = "AWS Secret Access Key (solo visible al crear la key)"
  type        = string
  sensitive   = true
}

variable "region" {
  description = <<-EOT
    Región AWS donde se despliega la infraestructura.
    us-east-1 (N. Virginia) tiene la mayor disponibilidad de instancias t3.
    Alternativas: eu-west-1, ap-southeast-1.
  EOT
  type    = string
  default = "us-east-1"
}

variable "ssh_public_key" {
  description = "Contenido de la clave SSH pública para acceso a las VMs. Ej: contenido de ~/.ssh/benchmark_aws.pub"
  type        = string
}
