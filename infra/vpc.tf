#===============================================================================
# VPC — Endpoint S3 (Gateway) exigido pelos workers do Glue
#===============================================================================

# Os jobs Glue rodam na VPC default em subnets PÚBLICAS, mas os ENIs dos
# workers NÃO recebem IP público — logo o tráfego de saída NÃO consegue
# atravessar o Internet Gateway (IGW exige IP público para NAT de saída).
#
# O endpoint S3 (Gateway) existia neste VPC e foi removido, fazendo o Glue
# falhar na validação de rede com:
#   "Could not find S3 endpoint or NAT gateway for subnetId ..."
#
# Este recurso recria o endpoint (sem custo) e adiciona a rota
# prefix-list -> vpce na main route table, restaurando o acesso ao S3 e a
# validação de VPC do Glue. Um endpoint S3 Gateway atende todo o tráfego S3
# do VPC (landing/raw/workspace/trusted/business) e também o script do job.

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = data.aws_vpc.default.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [data.aws_vpc.default.main_route_table_id]

  tags = merge(local.common_tags, {
    Name = "${var.glue_job_name}-s3-endpoint"
  })
}

# Os workers do Glue também precisam alcançar o STS e o Glue Data Catalog:
# o client factory do catálogo (AWSGlueDataCatalogHiveClientFactory) resolve
# credenciais via STS e chama a API do Glue. Como os ENIs dos workers não têm
# IP público, esse tráfego não atravessa o IGW — sem endpoint de interface (ou
# NAT) a chamada a sts.<region>.amazonaws.com falha com "Connect timed out".
# Este SG libera HTTPS (443) de entrada vindo do SG default (onde os workers
# estão), permitindo que eles usem os endpoints abaixo.

resource "aws_security_group" "vpc_endpoints" {
  name        = "${var.glue_job_name}-vpc-endpoints"
  description = "Allow Glue workers (default SG) to reach VPC interface endpoints"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "HTTPS from Glue workers (default SG)"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [data.aws_security_group.default.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${var.glue_job_name}-vpc-endpoints"
  })
}

resource "aws_vpc_endpoint" "sts" {
  vpc_id              = data.aws_vpc.default.id
  service_name        = "com.amazonaws.${var.region}.sts"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [data.aws_subnet.glue.id]
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = merge(local.common_tags, {
    Name = "${var.glue_job_name}-sts-endpoint"
  })
}

resource "aws_vpc_endpoint" "glue" {
  vpc_id              = data.aws_vpc.default.id
  service_name        = "com.amazonaws.${var.region}.glue"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [data.aws_subnet.glue.id]
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = merge(local.common_tags, {
    Name = "${var.glue_job_name}-glue-endpoint"
  })
}
