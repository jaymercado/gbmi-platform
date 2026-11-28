# The roles of the host (blueprint 5.9 and 9.2; week 3 plan 3.3 and difference D42).
#
#   gbmi-host                 the instance: the pipelines, Compose, the agents
#   gbmi-clickhouse-lake-ro   ClickHouse reads the lake through this role: warehouse/ only
#   gbmi-host-scheduler       EventBridge Scheduler starts and stops the host
#
# The policies of the host role are split by duty and name every prefix. No policy holds a
# wildcard resource for a write.

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.region
  partition  = data.aws_partition.current.partition

  glue_arn      = "arn:${local.partition}:glue:${local.region}:${local.account_id}"
  parameter_arn = "arn:${local.partition}:ssm:${local.region}:${local.account_id}:parameter${var.parameter_prefix}"
}

# --- gbmi-host --------------------------------------------------------------------------------

data "aws_iam_policy_document" "assume_host" {
  statement {
    sid     = "Ec2Instance"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "host" {
  name                 = "gbmi-host"
  description          = "The host of the platform: pipelines, Compose and the agents of AWS."
  assume_role_policy   = data.aws_iam_policy_document.assume_host.json
  max_session_duration = 3600
}

resource "aws_iam_instance_profile" "host" {
  name = "gbmi-host"
  role = aws_iam_role.host.name
}

# The lake. raw/ is written once and never deleted: the role can put an object and cannot
# delete one, and the bucket keeps versions. No statement names an object version.
data "aws_iam_policy_document" "host_lake" {
  statement {
    sid       = "ListLake"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.lake_bucket_arn]
  }

  statement {
    sid       = "ReadAndWriteRaw"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${var.lake_bucket_arn}/raw/*"]
  }

  statement {
    sid = "ReadWriteAndDeleteQuarantineAndWarehouse"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = [
      "${var.lake_bucket_arn}/quarantine/*",
      "${var.lake_bucket_arn}/warehouse/*",
    ]
  }
}

# The Glue Data Catalog: read the database, and read, create and update its tables. Terraform
# owns the database; the role cannot create or delete a database or delete a table.
data "aws_iam_policy_document" "host_glue" {
  statement {
    sid     = "ReadSilverDatabase"
    actions = ["glue:GetDatabase", "glue:GetDatabases"]
    resources = [
      "${local.glue_arn}:catalog",
      "${local.glue_arn}:database/${var.glue_database_name}",
    ]
  }

  statement {
    sid = "ReadCreateAndUpdateSilverTables"
    actions = [
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
    ]
    resources = [
      "${local.glue_arn}:catalog",
      "${local.glue_arn}:database/${var.glue_database_name}",
      "${local.glue_arn}:table/${var.glue_database_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "host_ops" {
  statement {
    sid       = "ListOps"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.ops_bucket_arn]
  }

  statement {
    sid     = "ReadAndWriteLogsArtifactsAndBackups"
    actions = ["s3:GetObject", "s3:PutObject"]
    resources = [
      "${var.ops_bucket_arn}/airflow-logs/*",
      "${var.ops_bucket_arn}/artifacts/*",
      "${var.ops_bucket_arn}/backups/*",
    ]
  }

  statement {
    sid       = "ReadWriteAndDeleteTmp"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${var.ops_bucket_arn}/tmp/*"]
  }
}

# The secrets of the host: read only, and only below the prefix of the environment. The
# entries are SecureString values under the key which AWS manages for Parameter Store.
data "aws_iam_policy_document" "host_parameters" {
  statement {
    sid       = "ReadHostParameters"
    actions   = ["ssm:GetParametersByPath", "ssm:GetParameters", "ssm:GetParameter"]
    resources = [local.parameter_arn, "${local.parameter_arn}/*"]
  }

  statement {
    sid       = "DecryptThroughParameterStore"
    actions   = ["kms:Decrypt"]
    resources = ["arn:${local.partition}:kms:${local.region}:${local.account_id}:key/*"]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ssm.${local.region}.amazonaws.com"]
    }

    condition {
      test     = "ForAnyValue:StringEquals"
      variable = "kms:ResourceAliases"
      values   = ["alias/aws/ssm"]
    }
  }
}

# The container logs (driver awslogs) and the logs of the host: write to the two log groups.
data "aws_iam_policy_document" "host_logs" {
  statement {
    sid       = "WriteToTheLogGroups"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
    resources = flatten([for arn in var.log_group_arns : [arn, "${arn}:log-stream:*"]])
  }
}

data "aws_iam_policy_document" "host_assume_lake_role" {
  statement {
    sid       = "AssumeTheLakeRoleForClickHouse"
    actions   = ["sts:AssumeRole"]
    resources = [aws_iam_role.clickhouse_lake.arn]
  }
}

resource "aws_iam_role_policy" "host" {
  for_each = {
    lake             = data.aws_iam_policy_document.host_lake.json
    glue             = data.aws_iam_policy_document.host_glue.json
    ops              = data.aws_iam_policy_document.host_ops.json
    parameters       = data.aws_iam_policy_document.host_parameters.json
    logs             = data.aws_iam_policy_document.host_logs.json
    assume-lake-role = data.aws_iam_policy_document.host_assume_lake_role.json
  }

  name   = each.key
  role   = aws_iam_role.host.id
  policy = each.value
}

# The two managed policies of AWS for the Systems Manager agent and the CloudWatch agent stay
# in phase P2. Both allow more than the split above; phase P4 replaces both (week 3 plan 3.3).
resource "aws_iam_role_policy_attachment" "host_agents" {
  for_each = toset(["AmazonSSMManagedInstanceCore", "CloudWatchAgentServerPolicy"])

  role       = aws_iam_role.host.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/${each.key}"
}

# --- gbmi-clickhouse-lake-ro -----------------------------------------------------------------
# A query of ClickHouse never uses the credentials of the server (ClickHouse 26.7 and later).
# The pin statement names this role; ClickHouse assumes it with the credentials of the host
# role. The role can list and read the Iceberg tables and nothing else: ClickHouse cannot
# write to the lake, and it cannot read the raw files or the quarantine.

data "aws_iam_policy_document" "assume_clickhouse_lake" {
  statement {
    sid     = "TheHostRole"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.host.arn]
    }
  }
}

resource "aws_iam_role" "clickhouse_lake" {
  name                 = "gbmi-clickhouse-lake-ro"
  description          = "ClickHouse on the host reads the Iceberg tables below warehouse/ of the lake bucket."
  assume_role_policy   = data.aws_iam_policy_document.assume_clickhouse_lake.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "clickhouse_lake" {
  statement {
    sid       = "ListWarehouse"
    actions   = ["s3:ListBucket"]
    resources = [var.lake_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["warehouse/*"]
    }
  }

  statement {
    sid       = "LocateTheBucket"
    actions   = ["s3:GetBucketLocation"]
    resources = [var.lake_bucket_arn]
  }

  statement {
    sid       = "ReadWarehouse"
    actions   = ["s3:GetObject"]
    resources = ["${var.lake_bucket_arn}/warehouse/*"]
  }
}

resource "aws_iam_role_policy" "clickhouse_lake" {
  name   = "read-warehouse"
  role   = aws_iam_role.clickhouse_lake.id
  policy = data.aws_iam_policy_document.clickhouse_lake.json
}

# --- gbmi-host-scheduler ---------------------------------------------------------------------
# EventBridge Scheduler stops the host each night and starts it each morning. The role can
# start and stop the one instance with the name of the host, and no other instance.

data "aws_iam_policy_document" "assume_scheduler" {
  statement {
    sid     = "EventBridgeScheduler"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "host_scheduler" {
  name                 = "gbmi-host-scheduler"
  description          = "EventBridge Scheduler starts and stops the host of the platform."
  assume_role_policy   = data.aws_iam_policy_document.assume_scheduler.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "host_scheduler" {
  statement {
    sid       = "StartAndStopTheHost"
    actions   = ["ec2:StartInstances", "ec2:StopInstances"]
    resources = ["arn:${local.partition}:ec2:${local.region}:${local.account_id}:instance/*"]

    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Name"
      values   = [var.host_name]
    }
  }
}

resource "aws_iam_role_policy" "host_scheduler" {
  name   = "start-and-stop-the-host"
  role   = aws_iam_role.host_scheduler.id
  policy = data.aws_iam_policy_document.host_scheduler.json
}
