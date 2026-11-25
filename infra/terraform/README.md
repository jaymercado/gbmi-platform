# Infrastructure

Terraform for the AWS side of the platform (blueprint §9.2, §9.4). Week 1 creates the baseline only: the state bucket and CI identities (`bootstrap/`), and the two buckets, the Glue database and the budget (`envs/prod/`).

> **Status: written and checked offline, not applied.** `make tf-check` passes (format, validate against AWS provider 6.74, tflint, checkov). No `plan` or `apply` has run against an AWS account yet, so the first apply is also the first real test of this code.

```text
infra/terraform/
├── bootstrap/        state bucket, GitHub OIDC provider, roles gbmi-ci-plan and gbmi-ci-deploy
├── modules/
│   ├── lake_s3/      lake and ops buckets, protections, lifecycle rules
│   ├── glue_catalog/ database gbmi_prod_silver with an explicit location
│   └── budgets/      monthly cost budget with notifications at 50, 80 and 100%
└── envs/prod/        composition of the three modules
```

## Checks that need no AWS access

```bash
make tf-check   # terraform fmt, validate, tflint, checkov
```

`.checkov.yaml` lists the four skipped policy checks with their reasons.

## Before the first apply

1. **AWS access.** A CLI profile with short-lived credentials (blueprint §5.9). Permission sets need an *organization* instance of IAM Identity Center; enabling it creates an AWS Organization, and on a free-plan account that ends the free credits. Decide that knowingly.
2. **The repository has to exist on GitHub first.** Repositories created after 15 July 2026 put the owner id and the repository id into the OIDC subject, and the CI roles trust exactly that subject.
3. **Local values.** Copy `local.env.example` to `local.env` (git-ignored) and fill it in. Look the ids up with `gh api repos/OWNER/gbmi-platform --jq '{owner_id: .owner.id, repository_id: .id}'`.
4. **Existing OIDC provider.** An account holds one provider per issuer. If `aws iam list-open-id-connect-providers` already shows `token.actions.githubusercontent.com`, apply the bootstrap with `-var create_github_oidc_provider=false`.

Every command below assumes credentials in the environment:

```bash
aws sso login --profile gbmi
eval "$(aws configure export-credentials --profile gbmi --format env)"
export AWS_DEFAULT_REGION=us-east-1
set -a; . infra/terraform/local.env; set +a
```

## 1. Bootstrap (once, by hand)

The state bucket cannot hold its own state before it exists, so the first apply uses local state and is then migrated.

```bash
cd infra/terraform/bootstrap

# First apply with local state. The override file is git-ignored.
printf 'terraform {\n  backend "local" {}\n}\n' > backend_override.tf
terraform init -input=false
terraform apply

# Move the state into the bucket that now exists.
rm backend_override.tf
cp backend.hcl.example backend.hcl        # put the account id in
terraform init -migrate-state -force-copy -input=false -backend-config=backend.hcl
rm -f terraform.tfstate terraform.tfstate.backup
terraform plan                            # expected: No changes
```

## 2. Production baseline

```bash
cd infra/terraform/envs/prod
cp backend.hcl.example backend.hcl        # put the account id in
terraform init -input=false -backend-config=backend.hcl
terraform apply
```

Afterwards `make plan` (from the repository root) must report no changes for both configurations.

## 3. Budget alert test

The acceptance criterion is a real alert email (week 1 plan, exit gate row 4).

1. **Confirm the recipient.** AWS sends a verification email after the first apply; follow its link (it expires after 12 hours and needs a console sign-in). Unverified recipients get no alerts.
2. **Lower the limit for the test:** `cp budget-test.auto.tfvars.example budget-test.auto.tfvars && terraform apply`.
3. **Create about USD 0.03 of real spend.** 5,000 S3 LIST requests cost the same as 5,000 PUT requests (USD 0.005 per 1,000) and leave nothing behind:

   ```bash
   seq 5000 | xargs -P 16 -I{} aws s3api list-objects-v2 \
     --bucket "$(terraform output -raw ops_bucket)" --prefix tmp/budget-test/ --max-keys 1 >/dev/null
   ```

   Billing data refreshes up to three times a day, so allow a day; do not start within a day of the month's end, because the spend would be counted in a period that is about to close.
4. **Check** the inbox and `aws budgets describe-budgets --account-id <id>` (the calculated spend shows there before the email arrives).
5. **Restore the limit:** `rm budget-test.auto.tfvars && terraform apply`.

The budget leaves credits and refunds out of the amount on purpose: by default AWS nets them off, and on an account with credits the spend would read as zero.

## Destroy path

`terraform destroy` removes everything except the buckets, which carry `prevent_destroy`. The lake survives and costs only storage (blueprint §9.4).
