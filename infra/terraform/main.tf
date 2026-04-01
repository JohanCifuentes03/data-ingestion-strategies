# ══════════════════════════════════════════════════════════════════
# infra/terraform/main.tf — Infraestructura AWS para experimento distribuido
#
# Despliega 4 EC2 instances en Amazon Web Services (x86_64):
#   VM-1 node-producers  10.0.1.10  t3.medium  (2 vCPU / 4 GB)  — Generator + Probe
#   VM-2 node-broker     10.0.1.20  t3.large   (2 vCPU / 8 GB)  — Kafka + ZooKeeper
#   VM-3 node-compute    10.0.1.30  t3.xlarge  (4 vCPU / 16 GB) — Spark + Flink
#   VM-4 node-sink       10.0.1.40  t3.medium  (2 vCPU / 4 GB)  — PostgreSQL + Prometheus
#
# Costo estimado con cuenta nueva AWS (us-east-1, precios On-Demand):
#   t3.medium:  $0.0464/h × 2 nodos = $0.09/h
#   t3.large:   $0.0928/h × 1 nodo  = $0.09/h
#   t3.xlarge:  $0.1856/h × 1 nodo  = $0.19/h
#   Total:                           ~ $0.37/hora
#   10 horas de experimento          ~ $3.70 USD
# ══════════════════════════════════════════════════════════════════

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
  }
}

# ── Provider AWS ─────────────────────────────────────────────────
provider "aws" {
  region     = var.region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
}

# ── Data Source: Ubuntu 22.04 LTS (x86_64) ──────────────────────
# Imagen oficial de Canonical (account ID: 099720109477).
# La expresión regular filtra únicamente las AMIs HVM con EBS por disco.
data "aws_ami" "ubuntu_22" {
  most_recent = true
  owners      = ["099720109477"] # Canonical official

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

# ── Key Pair: clave SSH del investigador ─────────────────────────
resource "aws_key_pair" "benchmark_key" {
  key_name   = "benchmark-key"
  public_key = var.ssh_public_key

  tags = {
    proyecto = "tesis-benchmark"
  }
}

# ── VPC (Virtual Private Cloud) ──────────────────────────────────
resource "aws_vpc" "benchmark_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name     = "benchmark-vpc"
    proyecto = "tesis-benchmark"
    entorno  = "experimento"
  }
}

# ── Internet Gateway ─────────────────────────────────────────────
resource "aws_internet_gateway" "benchmark_igw" {
  vpc_id = aws_vpc.benchmark_vpc.id

  tags = {
    Name     = "benchmark-igw"
    proyecto = "tesis-benchmark"
  }
}

# ── Route Table ──────────────────────────────────────────────────
resource "aws_route_table" "benchmark_rt" {
  vpc_id = aws_vpc.benchmark_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.benchmark_igw.id
  }

  tags = {
    Name     = "benchmark-route-table"
    proyecto = "tesis-benchmark"
  }
}

# ── Subnet Pública ───────────────────────────────────────────────
# Las instancias reciben IP pública automática para acceso SSH.
resource "aws_subnet" "benchmark_subnet" {
  vpc_id                  = aws_vpc.benchmark_vpc.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.region}a"
  map_public_ip_on_launch = true

  tags = {
    Name     = "benchmark-subnet"
    proyecto = "tesis-benchmark"
  }
}

# ── Asociación Route Table → Subnet ─────────────────────────────
resource "aws_route_table_association" "benchmark_rta" {
  subnet_id      = aws_subnet.benchmark_subnet.id
  route_table_id = aws_route_table.benchmark_rt.id
}

# ── Security Group ───────────────────────────────────────────────
# Configuración de Firewall (Security Group):
#   - SSH desde cualquier IP (investigador remoto)
#   - Todo TCP/UDP dentro de la subnet (comunicación entre nodos)
#   - Egreso total para descarga de imágenes Docker
resource "aws_security_group" "benchmark_sg" {
  name        = "benchmark-sg"
  description = "Security group para el benchmark de ingestion de datos"
  vpc_id      = aws_vpc.benchmark_vpc.id

  # SSH desde el exterior (investigador)
  ingress {
    description = "SSH acceso investigador"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Comunicación TCP interna entre los 4 nodos (Kafka, Spark, Flink, etc.)
  ingress {
    description = "Comunicacion interna TCP entre nodos del benchmark"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["10.0.1.0/24"]
  }

  # Comunicación UDP interna (NTP, DNS interno)
  ingress {
    description = "Comunicacion interna UDP entre nodos"
    from_port   = 0
    to_port     = 65535
    protocol    = "udp"
    cidr_blocks = ["10.0.1.0/24"]
  }

  # Egreso total: imágenes Docker, actualizaciones, NTP externo
  egress {
    description = "Egreso total sin restricciones"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name     = "benchmark-sg"
    proyecto = "tesis-benchmark"
  }
}

# ── VMs: 4 nodos del experimento ─────────────────────────────────

# VM-1: node-producers (Generator + Probe)
module "vm_producers" {
  source = "./modules/vm"

  ami_id            = data.aws_ami.ubuntu_22.id
  instance_type     = "c7i-flex.large"
  subnet_id         = aws_subnet.benchmark_subnet.id
  security_group_id = aws_security_group.benchmark_sg.id
  key_name          = aws_key_pair.benchmark_key.key_name
  private_ip        = "10.0.1.10"
  display_name      = "node-producers"
}

# VM-2: node-broker (Kafka + ZooKeeper)
# t3.large: 8 GB RAM para los logs de Kafka y el estado de ZooKeeper.
module "vm_broker" {
  source = "./modules/vm"

  ami_id            = data.aws_ami.ubuntu_22.id
  instance_type     = "m7i-flex.large"
  subnet_id         = aws_subnet.benchmark_subnet.id
  security_group_id = aws_security_group.benchmark_sg.id
  key_name          = aws_key_pair.benchmark_key.key_name
  private_ip        = "10.0.1.20"
  display_name      = "node-broker"
}

# VM-3: node-compute (Spark + Flink)
# t3.xlarge: 4 vCPU / 16 GB — la más potente del experimento.
module "vm_compute" {
  source = "./modules/vm"

  ami_id            = data.aws_ami.ubuntu_22.id
  instance_type     = "m7i-flex.large"
  subnet_id         = aws_subnet.benchmark_subnet.id
  security_group_id = aws_security_group.benchmark_sg.id
  key_name          = aws_key_pair.benchmark_key.key_name
  private_ip        = "10.0.1.30"
  display_name      = "node-compute"
}

# VM-4: node-sink (PostgreSQL + Prometheus + cAdvisor)
module "vm_sink" {
  source = "./modules/vm"

  ami_id            = data.aws_ami.ubuntu_22.id
  instance_type     = "c7i-flex.large"
  subnet_id         = aws_subnet.benchmark_subnet.id
  security_group_id = aws_security_group.benchmark_sg.id
  key_name          = aws_key_pair.benchmark_key.key_name
  private_ip        = "10.0.1.40"
  display_name      = "node-sink"
}
