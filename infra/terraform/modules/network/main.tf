# The network of the host (blueprint 5.9 and 9.2; week 3 plan 3.3).
#
# One VPC with one public subnet, an internet gateway and a gateway endpoint for S3. There is no
# NAT gateway: the host has a public address and reaches GDELT, the SEC, GitHub, the
# registries and AWS over HTTPS. The security group of the host has no inbound rule at all;
# Session Manager is the only way in (week 3 plan, exit-gate row 4).

data "aws_region" "current" {}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.name_prefix}-vpc"
  }
}

# The default security group of a VPC allows all traffic between its members. Adopting it
# without a rule leaves it with none, so a resource which lands in it by mistake is closed.
resource "aws_default_security_group" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${var.name_prefix}-default-closed"
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${var.name_prefix}-igw"
  }
}

# The subnet gives no public address by itself: the host asks for one at its launch.
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = var.public_subnet_cidr
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.name_prefix}-public"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = {
    Name = "${var.name_prefix}-public"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Traffic to S3 stays inside the network of AWS and costs no data transfer.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${data.aws_region.current.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]

  tags = {
    Name = "${var.name_prefix}-s3"
  }
}

resource "aws_security_group" "host" {
  name        = "${var.name_prefix}-host"
  description = "The host: no inbound rule; outbound HTTPS only."
  vpc_id      = aws_vpc.this.id

  tags = {
    Name = "${var.name_prefix}-host"
  }
}

# Every source and every service of the platform answers on HTTPS. The name service, the
# time service and the metadata service of AWS are not filtered by a security group.
resource "aws_vpc_security_group_egress_rule" "host_https" {
  security_group_id = aws_security_group.host.id
  description       = "HTTPS to every address: GDELT, the SEC, GitHub, the registries and AWS."
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}
