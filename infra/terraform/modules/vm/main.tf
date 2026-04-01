# ══════════════════════════════════════════════════════════════════
# infra/terraform/modules/vm/main.tf — Módulo reutilizable EC2 (AWS)
#
# Crea una instancia EC2 con:
#   - AMI de Ubuntu 22.04 LTS (amd64)
#   - IP privada estática dentro de la subnet del benchmark
#   - IP pública automática para acceso SSH
#   - Disco raíz de 50 GB (gp3, más rápido y barato que gp2)
# ══════════════════════════════════════════════════════════════════

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "ami_id"            { type = string }
variable "instance_type"     { type = string }
variable "subnet_id"         { type = string }
variable "security_group_id" { type = string }
variable "key_name"          { type = string }
variable "private_ip"        { type = string }
variable "display_name"      { type = string }

resource "aws_instance" "vm" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.security_group_id]
  key_name               = var.key_name
  private_ip             = var.private_ip

  root_block_device {
    volume_size           = 20
    volume_type           = "gp3"
    delete_on_termination = true
  }

  # Cloud-init: configura ubuntu como sudoer sin contraseña.
  # Esto es equivalente al comportamiento normal de Ubuntu sin contraseña root.
  user_data = base64encode(<<-EOT
    #!/bin/bash
    echo 'ubuntu ALL=(ALL) NOPASSWD:ALL' | tee /etc/sudoers.d/ubuntu-nopasswd
    chmod 440 /etc/sudoers.d/ubuntu-nopasswd
  EOT
  )

  tags = {
    Name     = var.display_name
    proyecto = "tesis-benchmark"
    nodo     = var.display_name
  }

  # Esperar hasta que la instancia esté completamente disponible
  timeouts {
    create = "10m"
  }
}

output "private_ip" {
  value = aws_instance.vm.private_ip
}

output "public_ip" {
  value = aws_instance.vm.public_ip
}
