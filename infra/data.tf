#===============================================================================
# Data Sources — Existing AWS resources used by the Glue job
#===============================================================================

# ── AWS Account ───────────────────────────────────────────────────────────────

data "aws_caller_identity" "current" {}

# ── VPC ──────────────────────────────────────────────────────────────────────

data "aws_vpc" "default" {
  default = true
}

# ── Subnets ──────────────────────────────────────────────────────────────────

data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "map-public-ip-on-launch"
    values = [false]
  }
}

data "aws_subnets" "public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "map-public-ip-on-launch"
    values = [true]
  }
}

data "aws_subnets" "all" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Subnet única usada pela conexão Glue — fonte única de verdade para AZ e
# subnet_id. Prefere subnets privadas; caso contrário usa qualquer subnet.
# sort() garante seleção determinística (o data source aws_subnets não
# garante ordem estável na lista ids), evitando descasamento entre a
# availability_zone e o subnet_id da conexão.
data "aws_subnet" "glue" {
  id = length(data.aws_subnets.private.ids) > 0 ? sort(data.aws_subnets.private.ids)[0] : sort(data.aws_subnets.all.ids)[0]
}

# ── Security Group ────────────────────────────────────────────────────────────

data "aws_security_group" "default" {
  vpc_id = data.aws_vpc.default.id
  name   = "default"
}
