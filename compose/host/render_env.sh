#!/usr/bin/env bash
# Writes the environment file of Compose from Parameter Store (week 3 plan 3.3).
#
#   compose/host/render_env.sh <commit>
#
# The role of the host reads the SecureString parameters below /gbmi/prod/: the secrets of
# .env.example, the declared identity for the SEC and the source of the SEC client. The
# bucket names and the address of the lake role come from the account of the host, so the
# account number stands in no file of the repository. The file is readable by root only.
# The script prints names and counts, never a value.
set -euo pipefail
umask 077

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=compose/host/common.sh
. "$here/common.sh"

commit="${1:-}"
require_commit "$commit"

account="$(aws sts get-caller-identity --query Account --output text)" \
  || die "the identity of the host cannot be read"

# The answer holds the values: it goes through a pipe into the script which writes the file,
# and never into a variable, an argument or a file of its own.
aws ssm get-parameters-by-path --path "$GBMI_PARAMETER_PREFIX" --recursive --with-decryption \
  --output json \
  | GBMI_RELEASE="$commit" GBMI_ACCOUNT="$account" python3 "$here/render_env.py" "$GBMI_ENV_FILE"
