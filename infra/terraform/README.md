# Infrastructure

Terraform for the AWS side of the platform (blueprint §9.2, §9.4). Week 1 created the baseline: the state bucket and the CI identities (`bootstrap/`), and the two buckets, the Glue database and the budget (`envs/prod/`). Week 3 adds the host: a network, three roles, one EC2 instance with its schedule and its deployment command, and the monitoring.

> **Status.** The baseline of week 1 was applied on 2026-11-26 in `us-east-1`: 13 resources in `bootstrap/` and 16 in `envs/prod/`. The resources of week 3 are code until the two applies of work package WP-11; the status report `docs/reports/p2-sec-and-deployment.md` records the applies. `make tf-check` passes (format, validate against AWS provider 6.74, tflint, checkov).

```text
infra/terraform/
├── bootstrap/        state bucket, GitHub OIDC provider, roles gbmi-ci-plan and gbmi-ci-deploy
├── modules/
│   ├── lake_s3/      lake and ops buckets, protections, lifecycle rules
│   ├── glue_catalog/ database gbmi_prod_silver with an explicit location
│   ├── budgets/      monthly cost budget with notifications at 50, 80 and 100%
│   ├── network/      VPC, one public subnet, internet gateway, S3 gateway endpoint, the security group of the host
│   ├── iam/          roles gbmi-host, gbmi-clickhouse-lake-ro and gbmi-host-scheduler
│   ├── ec2_host/     the instance with its user data, the two schedules, the command document gbmi-prod-deploy
│   └── monitoring/   two log groups, the alert topic, five alarms
└── envs/prod/        composition of the seven modules
```

## Checks that need no AWS access

```bash
make tf-check         # terraform fmt, validate, tflint, checkov
make host-rehearsal   # the user data and the host scripts in a container of Amazon Linux 2023 (arm64)
```

`.checkov.yaml` lists the twelve skipped policy checks with their reasons: four of week 1 and eight of week 3. One more skip stands beside its resource in `bootstrap/github_oidc.tf`.

`make host-rehearsal` renders the user data as Terraform does and runs it in a container: the real packages, the real file of the Compose plugin with its checksum and the real validation of the CloudWatch agent configuration. Then the rehearsal runs three deployments, each refusal of the deployment script and the start at boot, with stand-in commands for `docker`, `aws` and `systemctl`. Run the rehearsal before a change of the user data or of a host script reaches an instance: an instance ignores a later change of its user data, so a defect of the user data costs a replacement of the instance. The rehearsal needs Docker on an arm64 machine. It does not cover cloud-init, systemd, swap, the roles of AWS and the real Compose stack.

## The job `plan` of the workflow `terraform`

A pull request of the repository runs `terraform plan` for both configurations with the role `gbmi-ci-plan`. The role can read the state and the resources of the project, and nothing else. The log of the job is public, so the job shows one result line for each configuration and, after a failure, the error messages of Terraform; the text of a plan is never shown (`tools/ci_plan.py`). The job takes no state lock: a job which GitHub cancels halfway would leave the lock file behind. The job is not a required check of `main`.

A new resource type needs a read action in the policy of the role (`bootstrap/github_oidc.tf`) before the job can read it. The job then fails until a person has applied `bootstrap`.

## Before the first apply

1. **AWS access.** A CLI profile with short-lived credentials (blueprint §5.9). This project signs in as the root user of the account with `aws login` (week 1 plan, D13): the profile `gbmi-login` holds the sign-in, and the profile `gbmi` hands its credentials to Terraform and to every other program. The root user has MFA and no access key.
2. **The repository has to exist on GitHub first.** Repositories created after 15 July 2026 put the owner id and the repository id into the OIDC subject, and the CI roles trust exactly that subject.
3. **Local values.** Copy `local.env.example` to `local.env` (git-ignored) and fill it in. Look the ids up with `gh api repos/OWNER/gbmi-platform --jq '{owner_id: .owner.id, repository_id: .id}'`.
4. **Existing OIDC provider.** An account holds one provider per issuer. If `aws iam list-open-id-connect-providers` already shows `token.actions.githubusercontent.com`, apply the bootstrap with `-var create_github_oidc_provider=false`.

The two profiles are created once:

```bash
aws configure set region us-east-1 --profile gbmi-login
aws login --remote --profile gbmi-login   # open the printed address in a browser where the root user is signed in
aws configure set credential_process \
    "aws configure export-credentials --profile gbmi-login --format process" --profile gbmi
aws configure set region us-east-1 --profile gbmi
```

A sign-in lasts 12 hours at the latest; after that, `aws login --remote --profile gbmi-login` again. Every command below assumes:

```bash
export AWS_PROFILE=gbmi
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

## 2b. The host (week 3)

The same two commands apply the host: `terraform apply` in `envs/prod`, then `terraform apply` in `bootstrap` for the two CI roles. No workflow applies Terraform: a person reads the plan and applies on the laptop (week 3 plan, D40).

- **Cost.** The apply of `envs/prod` starts the instance, and the cost of the host starts with it: a `t4g.large` spot instance, 16 hours a day.
- **Variables** of `envs/prod`, each with a default. Three are fixed when the host is created, so choose them before the first apply: `host_market` (`spot`, or `on_demand` when the first apply finds no spot capacity), `host_zone` and `host_instance_type`. A later change of one of the three does not reach the host: Terraform refuses to replace the instance, the change from `spot` to `on_demand` shows no difference, and AWS does not change the type of a spot instance. `host_schedule_enabled` (the host stops at 24:00 UTC and starts at 08:00 UTC) can change at any time. `TF_VAR_github_repository` of `local.env` now serves both configurations.
- **Not in the hour before 24:00 UTC.** The schedule stops the host at 24:00 UTC. The first boot of an instance runs once: an apply shortly before the stop would cut it. After the apply, the log group `/gbmi/prod/host` has to show the line `gbmi user data: done`.
- **One email.** AWS sends one email for the alert topic. The alarms deliver nothing before the link of the email is opened.
- **The instance is never replaced by accident.** The operational database of the host has no backup before phase P4. Three guards stand in `modules/ec2_host/main.tf`: Terraform refuses every plan which would destroy or replace the instance (`prevent_destroy`); the instance ignores a newer image, a changed user data script and the missing public address of a stopped instance (`ignore_changes`); the disk outlives the instance (`delete_on_termination = false`). Read every plan for the line `must be replaced` before an apply. Before an apply which stops the instance: one `pg_dump` of the operational database to the prefix `backups/` of the ops bucket.
- **Never in the AWS console:** do not cancel the spot request of the host, and do not terminate the instance. AWS terminates a stopped spot instance when its request is cancelled.
- **The secrets of the host are no part of Terraform.** `make host-secrets` writes the entries below `/gbmi/prod/` of Parameter Store, so no secret stands in the state (week 3 plan, D43).
- **After the apply:** `make plan` reports no changes; `make host-status` shows the instance and the agent; `docs/runbooks/deploy-and-roll-back.md` has the steps of a deployment.

## 3. Budget alert test

The test is optional. Since 2026-11-26 an alert email is not a condition of P0 (week 1 plan, D12). The test proves that an alert email is delivered.

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

`terraform destroy` removes everything except the buckets, which carry `prevent_destroy`. The lake survives and costs only storage (blueprint §9.4). The instance of the host also carries `prevent_destroy`: remove the line in `modules/ec2_host/main.tf` first. The disk of the host stays after the instance is gone and costs storage until a person deletes the volume; the disk holds the operational database.
