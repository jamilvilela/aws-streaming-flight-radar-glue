#===============================================================================
# Data Sources — Existing AWS resources used by the Glue job
#===============================================================================

# ── IAM Role ──────────────────────────────────────────────────────────────────

data "aws_iam_role" "datalake_analytics" {
  name = var.glue_iam_role_name
}

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

data "aws_subnet" "all" {
  for_each = toset(data.aws_subnets.all.ids)
  id       = each.value
}

# ── Security Group ────────────────────────────────────────────────────────────

data "aws_security_group" "default" {
  vpc_id = data.aws_vpc.default.id
  name   = "default"
}
