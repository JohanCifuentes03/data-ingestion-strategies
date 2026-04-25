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

provider "aws" {
  alias      = "brazil"
  region     = var.brazil_region
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

data "aws_ami" "ubuntu_22_brazil" {
  provider    = aws.brazil
  most_recent = true
  owners      = ["099720109477"]

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

resource "aws_key_pair" "benchmark_key_brazil" {
  provider   = aws.brazil
  count      = var.enable_brazil_compute ? 1 : 0
  key_name   = "benchmark-key-brazil"
  public_key = var.ssh_public_key

  tags = {
    proyecto = "tesis-benchmark"
  }
}

# ── IAM para CloudWatch Agent (métricas host-level) ──────────────
resource "aws_iam_role" "ec2_cloudwatch_role" {
  name = "tesis-benchmark-ec2-cloudwatch-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    proyecto = "tesis-benchmark"
  }
}

resource "aws_iam_role_policy_attachment" "ec2_cloudwatch_agent_server_policy" {
  role       = aws_iam_role.ec2_cloudwatch_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

resource "aws_iam_role_policy_attachment" "ec2_cloudwatch_readonly_policy" {
  role       = aws_iam_role.ec2_cloudwatch_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchReadOnlyAccess"
}

resource "aws_iam_role_policy" "ec2_cloudwatch_read_policy" {
  name = "tesis-benchmark-cloudwatch-read"
  role = aws_iam_role.ec2_cloudwatch_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:ListMetrics"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "ec2_cloudwatch_profile" {
  name = "tesis-benchmark-ec2-cloudwatch-profile"
  role = aws_iam_role.ec2_cloudwatch_role.name
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

resource "aws_vpc" "benchmark_vpc_brazil" {
  provider             = aws.brazil
  count                = var.enable_brazil_compute ? 1 : 0
  cidr_block           = var.brazil_vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name     = "benchmark-vpc-brazil"
    proyecto = "tesis-benchmark"
    entorno  = "experimento-avanzado"
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

resource "aws_internet_gateway" "benchmark_igw_brazil" {
  provider = aws.brazil
  count    = var.enable_brazil_compute ? 1 : 0
  vpc_id   = aws_vpc.benchmark_vpc_brazil[0].id

  tags = {
    Name     = "benchmark-igw-brazil"
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

resource "aws_route_table" "benchmark_rt_brazil" {
  provider = aws.brazil
  count    = var.enable_brazil_compute ? 1 : 0
  vpc_id   = aws_vpc.benchmark_vpc_brazil[0].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.benchmark_igw_brazil[0].id
  }

  tags = {
    Name     = "benchmark-route-table-brazil"
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

resource "aws_subnet" "benchmark_subnet_brazil" {
  provider                = aws.brazil
  count                   = var.enable_brazil_compute ? 1 : 0
  vpc_id                  = aws_vpc.benchmark_vpc_brazil[0].id
  cidr_block              = var.brazil_subnet_cidr
  availability_zone       = "${var.brazil_region}a"
  map_public_ip_on_launch = true

  tags = {
    Name     = "benchmark-subnet-brazil"
    proyecto = "tesis-benchmark"
  }
}

# ── Asociación Route Table → Subnet ─────────────────────────────
resource "aws_route_table_association" "benchmark_rta" {
  subnet_id      = aws_subnet.benchmark_subnet.id
  route_table_id = aws_route_table.benchmark_rt.id
}

resource "aws_route_table_association" "benchmark_rta_brazil" {
  provider       = aws.brazil
  count          = var.enable_brazil_compute ? 1 : 0
  subnet_id      = aws_subnet.benchmark_subnet_brazil[0].id
  route_table_id = aws_route_table.benchmark_rt_brazil[0].id
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

resource "aws_security_group_rule" "primary_from_brazil_tcp" {
  count             = var.enable_brazil_compute ? 1 : 0
  type              = "ingress"
  description       = "Advanced experiment traffic from Brazil compute VPC"
  from_port         = 0
  to_port           = 65535
  protocol          = "tcp"
  cidr_blocks       = [var.brazil_vpc_cidr]
  security_group_id = aws_security_group.benchmark_sg.id
}

resource "aws_security_group_rule" "primary_from_brazil_udp" {
  count             = var.enable_brazil_compute ? 1 : 0
  type              = "ingress"
  description       = "Advanced experiment UDP from Brazil compute VPC"
  from_port         = 0
  to_port           = 65535
  protocol          = "udp"
  cidr_blocks       = [var.brazil_vpc_cidr]
  security_group_id = aws_security_group.benchmark_sg.id
}

resource "aws_security_group_rule" "primary_public_kafka_from_brazil_compute" {
  count             = var.enable_brazil_compute ? 1 : 0
  type              = "ingress"
  description       = "Public Kafka access from Brazil compute"
  from_port         = 19092
  to_port           = 19092
  protocol          = "tcp"
  cidr_blocks       = ["${module.vm_compute_brazil[0].public_ip}/32"]
  security_group_id = aws_security_group.benchmark_sg.id
}

resource "aws_security_group_rule" "primary_public_postgres_from_brazil_compute" {
  count             = var.enable_brazil_compute ? 1 : 0
  type              = "ingress"
  description       = "Public PostgreSQL access from Brazil compute"
  from_port         = 5432
  to_port           = 5432
  protocol          = "tcp"
  cidr_blocks       = ["${module.vm_compute_brazil[0].public_ip}/32"]
  security_group_id = aws_security_group.benchmark_sg.id
}

resource "aws_security_group" "benchmark_sg_brazil" {
  provider    = aws.brazil
  count       = var.enable_brazil_compute ? 1 : 0
  name        = "benchmark-sg-brazil"
  description = "Security group para compute node en Brasil"
  vpc_id      = aws_vpc.benchmark_vpc_brazil[0].id

  ingress {
    description = "SSH access"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Traffic from primary benchmark VPC"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.benchmark_vpc.cidr_block]
  }

  ingress {
    description = "UDP from primary benchmark VPC"
    from_port   = 0
    to_port     = 65535
    protocol    = "udp"
    cidr_blocks = [aws_vpc.benchmark_vpc.cidr_block]
  }

  egress {
    description = "Egreso total sin restricciones"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name     = "benchmark-sg-brazil"
    proyecto = "tesis-benchmark"
  }
}

resource "aws_security_group_rule" "brazil_metrics_from_sink_public" {
  provider          = aws.brazil
  count             = var.enable_brazil_compute ? 1 : 0
  type              = "ingress"
  description       = "Prometheus and metrics from sink public IP"
  from_port         = 8081
  to_port           = 9250
  protocol          = "tcp"
  cidr_blocks       = ["${module.vm_sink.public_ip}/32"]
  security_group_id = aws_security_group.benchmark_sg_brazil[0].id
}

resource "aws_vpc_peering_connection" "primary_to_brazil" {
  count       = var.enable_brazil_compute ? 1 : 0
  vpc_id      = aws_vpc.benchmark_vpc.id
  peer_vpc_id = aws_vpc.benchmark_vpc_brazil[0].id
  peer_region = var.brazil_region
  auto_accept = false

  tags = {
    Name     = "benchmark-primary-to-brazil"
    proyecto = "tesis-benchmark"
  }
}

resource "aws_vpc_peering_connection_accepter" "brazil_accept" {
  provider                  = aws.brazil
  count                     = var.enable_brazil_compute ? 1 : 0
  vpc_peering_connection_id = aws_vpc_peering_connection.primary_to_brazil[0].id
  auto_accept               = true

  tags = {
    Name     = "benchmark-brazil-accept"
    proyecto = "tesis-benchmark"
  }
}

resource "aws_route" "primary_to_brazil" {
  count                     = var.enable_brazil_compute ? 1 : 0
  route_table_id            = aws_route_table.benchmark_rt.id
  destination_cidr_block    = var.brazil_vpc_cidr
  vpc_peering_connection_id = aws_vpc_peering_connection.primary_to_brazil[0].id
}

resource "aws_route" "brazil_to_primary" {
  provider                  = aws.brazil
  count                     = var.enable_brazil_compute ? 1 : 0
  route_table_id            = aws_route_table.benchmark_rt_brazil[0].id
  destination_cidr_block    = aws_vpc.benchmark_vpc.cidr_block
  vpc_peering_connection_id = aws_vpc_peering_connection.primary_to_brazil[0].id
}

# ── VMs: 4 nodos del experimento ─────────────────────────────────

# VM-1: node-producers (Generator + Probe)
module "vm_producers" {
  source = "./modules/vm"

  ami_id                    = data.aws_ami.ubuntu_22.id
  instance_type             = "c7i-flex.large"
  subnet_id                 = aws_subnet.benchmark_subnet.id
  security_group_id         = aws_security_group.benchmark_sg.id
  key_name                  = aws_key_pair.benchmark_key.key_name
  private_ip                = "10.0.1.10"
  display_name              = "node-producers"
  iam_instance_profile_name = aws_iam_instance_profile.ec2_cloudwatch_profile.name
}

# VM-2: node-broker (Kafka + ZooKeeper)
# t3.large: 8 GB RAM para los logs de Kafka y el estado de ZooKeeper.
module "vm_broker" {
  source = "./modules/vm"

  ami_id                    = data.aws_ami.ubuntu_22.id
  instance_type             = "m7i-flex.large"
  subnet_id                 = aws_subnet.benchmark_subnet.id
  security_group_id         = aws_security_group.benchmark_sg.id
  key_name                  = aws_key_pair.benchmark_key.key_name
  private_ip                = "10.0.1.20"
  display_name              = "node-broker"
  iam_instance_profile_name = aws_iam_instance_profile.ec2_cloudwatch_profile.name
}

# VM-3: node-compute (Spark + Flink)
# t3.xlarge: 4 vCPU / 16 GB — la más potente del experimento.
module "vm_compute" {
  source = "./modules/vm"

  ami_id                    = data.aws_ami.ubuntu_22.id
  instance_type             = "m7i-flex.large"
  subnet_id                 = aws_subnet.benchmark_subnet.id
  security_group_id         = aws_security_group.benchmark_sg.id
  key_name                  = aws_key_pair.benchmark_key.key_name
  private_ip                = "10.0.1.30"
  display_name              = "node-compute"
  iam_instance_profile_name = aws_iam_instance_profile.ec2_cloudwatch_profile.name
}

# VM-4: node-sink (PostgreSQL + Prometheus + cAdvisor)
module "vm_sink" {
  source = "./modules/vm"

  ami_id                    = data.aws_ami.ubuntu_22.id
  instance_type             = "c7i-flex.large"
  subnet_id                 = aws_subnet.benchmark_subnet.id
  security_group_id         = aws_security_group.benchmark_sg.id
  key_name                  = aws_key_pair.benchmark_key.key_name
  private_ip                = "10.0.1.40"
  display_name              = "node-sink"
  iam_instance_profile_name = aws_iam_instance_profile.ec2_cloudwatch_profile.name
}

module "vm_compute_brazil" {
  source = "./modules/vm"
  count  = var.enable_brazil_compute ? 1 : 0

  providers = {
    aws = aws.brazil
  }

  ami_id                    = data.aws_ami.ubuntu_22_brazil.id
  instance_type             = "m7i-flex.large"
  subnet_id                 = aws_subnet.benchmark_subnet_brazil[0].id
  security_group_id         = aws_security_group.benchmark_sg_brazil[0].id
  key_name                  = aws_key_pair.benchmark_key_brazil[0].key_name
  private_ip                = "10.1.1.30"
  display_name              = "node-compute-brazil"
  iam_instance_profile_name = aws_iam_instance_profile.ec2_cloudwatch_profile.name
}
