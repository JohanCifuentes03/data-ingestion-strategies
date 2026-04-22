# ══════════════════════════════════════════════════════════════════
# infra/terraform/outputs.tf — Salidas de infraestructura AWS
#
# Exporta:
#   1. IPs privadas de las 4 VMs (usadas por Ansible y los scripts)
#   2. IPs públicas de las 4 VMs (usadas para SSH desde el exterior)
#   3. infra/ansible/inventory.ini (generado automáticamente para Ansible)
#   4. infra/terraform/outputs.env (variables shell para los scripts de experimento)
# ══════════════════════════════════════════════════════════════════

# ── IPs Privadas (red interna benchmark-net 10.0.1.0/24) ────────
output "vm_producers_ip" {
  description = "IP privada del nodo generator/probe (VM-1)"
  value       = module.vm_producers.private_ip
}

output "vm_producers_instance_id" {
  description = "Instance ID del nodo generator/probe (VM-1)"
  value       = module.vm_producers.instance_id
}

output "vm_broker_ip" {
  description = "IP privada del nodo Kafka broker (VM-2)"
  value       = module.vm_broker.private_ip
}

output "vm_broker_instance_id" {
  description = "Instance ID del nodo Kafka broker (VM-2)"
  value       = module.vm_broker.instance_id
}

output "vm_compute_ip" {
  description = "IP privada del nodo Spark/Flink (VM-3)"
  value       = module.vm_compute.private_ip
}

output "vm_compute_instance_id" {
  description = "Instance ID del nodo Spark/Flink (VM-3)"
  value       = module.vm_compute.instance_id
}

output "vm_sink_ip" {
  description = "IP privada del nodo PostgreSQL/Prometheus (VM-4)"
  value       = module.vm_sink.private_ip
}

output "vm_sink_instance_id" {
  description = "Instance ID del nodo PostgreSQL/Prometheus (VM-4)"
  value       = module.vm_sink.instance_id
}

# ── IPs Públicas (acceso SSH desde el exterior) ─────────────────
output "vm_producers_public_ip" {
  description = "IP pública del nodo generator/probe — para SSH"
  value       = module.vm_producers.public_ip
}

output "vm_broker_public_ip" {
  description = "IP pública del nodo Kafka broker — para SSH"
  value       = module.vm_broker.public_ip
}

output "vm_compute_public_ip" {
  description = "IP pública del nodo Spark/Flink — para SSH"
  value       = module.vm_compute.public_ip
}

output "vm_sink_public_ip" {
  description = "IP pública del nodo PostgreSQL/Prometheus — para SSH"
  value       = module.vm_sink.public_ip
}

# ── Ansible Inventory (generado automáticamente) ────────────────
# Se escribe directamente en infra/ansible/inventory.ini tras terraform apply.
# NO editar a mano: se sobreescribe en cada apply.
resource "local_file" "ansible_inventory" {
  filename        = "${path.module}/../ansible/inventory.ini"
  file_permission = "0644"

  content = templatefile("${path.module}/templates/inventory.ini.tpl", {
    vm_producers_public_ip = module.vm_producers.public_ip
    vm_broker_public_ip    = module.vm_broker.public_ip
    vm_compute_public_ip   = module.vm_compute.public_ip
    vm_sink_public_ip      = module.vm_sink.public_ip
    vm_producers_ip        = module.vm_producers.private_ip
    vm_broker_ip           = module.vm_broker.private_ip
    vm_compute_ip          = module.vm_compute.private_ip
    vm_sink_ip             = module.vm_sink.private_ip
  })
}

# ── Shell Environment File (para scripts de experimento) ─────────
# Cargado por thesis.sh y experiment.sh cuando MODE=distributed.
# Nunca se sube al repositorio (.gitignore).
resource "local_file" "outputs_env" {
  filename        = "${path.module}/outputs.env"
  file_permission = "0600"

  content = <<-EOT
    # Generado automáticamente por terraform apply — NO editar a mano
    # Cargar con: source infra/terraform/outputs.env
    export CLOUD_VM_PRODUCER_IP="${module.vm_producers.private_ip}"
    export CLOUD_VM_BROKER_IP="${module.vm_broker.private_ip}"
    export CLOUD_VM_COMPUTE_IP="${module.vm_compute.private_ip}"
    export CLOUD_VM_SINK_IP="${module.vm_sink.private_ip}"
    export CLOUD_VM_PRODUCER_PUBLIC_IP="${module.vm_producers.public_ip}"
    export CLOUD_VM_BROKER_PUBLIC_IP="${module.vm_broker.public_ip}"
    export CLOUD_VM_COMPUTE_PUBLIC_IP="${module.vm_compute.public_ip}"
    export CLOUD_VM_SINK_PUBLIC_IP="${module.vm_sink.public_ip}"
    export CLOUD_VM_PRODUCER_INSTANCE_ID="${module.vm_producers.instance_id}"
    export CLOUD_VM_BROKER_INSTANCE_ID="${module.vm_broker.instance_id}"
    export CLOUD_VM_COMPUTE_INSTANCE_ID="${module.vm_compute.instance_id}"
    export CLOUD_VM_SINK_INSTANCE_ID="${module.vm_sink.instance_id}"
    export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${module.vm_broker.private_ip}:9092"
    export KAFKA_BOOTSTRAP_SERVERS="${module.vm_broker.private_ip}:9092"
  EOT
}

# ── Resumen de conexión SSH ──────────────────────────────────────
output "ssh_commands" {
  description = "Comandos SSH para conectarse a cada nodo"
  value = {
    producers = "ssh -i ~/.ssh/benchmark_aws ubuntu@${module.vm_producers.public_ip}"
    broker    = "ssh -i ~/.ssh/benchmark_aws ubuntu@${module.vm_broker.public_ip}"
    compute   = "ssh -i ~/.ssh/benchmark_aws ubuntu@${module.vm_compute.public_ip}"
    sink      = "ssh -i ~/.ssh/benchmark_aws ubuntu@${module.vm_sink.public_ip}"
  }
}
