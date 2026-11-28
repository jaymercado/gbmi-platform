# ADR-011 — Cloud topology and cost controls: one spot host which stops at night

| | |
|---|---|
| **Date** | 2026-11-28 |
| **Status** | **Provisional** until the cost report of phase P4. The network, the roles, the host and the monitoring were applied on 2026-11-28; the record is updated after the measurements of work package WP-13 |
| **Sources** | Blueprint §5.9, §9.2, §9.4, §9.7, §12, technology decisions TD-12, TD-13 and TD-14; week 1 plan, differences D12 and D13; week 3 plan §3.3, differences D40 to D45, changes C16, C26 and C27 |
| **Related** | Review 3 of the status report of phase P2; the runbook `docs/runbooks/deploy-and-roll-back.md`; `infra/terraform/README.md`; ADR-010 |

## Context

The platform has to run without a person watching it, on AWS, for about 30 US dollars a month. One developer builds and operates it. The data comes from public sources, so the platform holds no personal data and no data of a customer. The platform is a portfolio project: a day without the platform costs nothing but the day.

The stack is the one of the laptop: Airflow with four processes, PostgreSQL for the operational tables, ClickHouse for the marts, and later Metabase. The measurements of phase P0 put the core of the stack at about 1.9 GB of memory when idle, and at about half of its memory budget of 7.5 GB during the demo (`docs/reports/p0-memory.md`).

The usual shapes of such a platform on AWS do not fit the budget. A managed Airflow environment alone costs about 358 US dollars a month in its smallest size. A NAT gateway, which a private network needs to reach the internet, costs about 33 US dollars a month.

Five points need a decision: where the stack runs; how the machine reaches the internet and how a person reaches the machine; where the secrets live; how a change reaches AWS; and what keeps the cost inside the budget.

## Decision

**1. One region and one machine.** Everything lives in `us-east-1`. One EC2 instance of the type `t4g.large` (arm64, 2 vCPUs, 8 GB) runs the whole Compose stack. The lake is on S3 with the Glue Data Catalog; the operational database and ClickHouse live on the encrypted disk of the instance.

**2. A spot instance which stops at night.** The instance is a persistent spot request: on an interruption AWS stops the instance and keeps its disk. EventBridge Scheduler stops the instance at 24:00 UTC and starts it at 08:00 UTC each day, so the host runs 16 hours a day. The CPU credits are unlimited, so the host never slows down; an alarm watches the surplus credits which AWS would charge at the next stop.

**3. A public subnet without a way in.** The host has a public address and no NAT gateway. Its security group has no inbound rule and one outbound rule, HTTPS. The instance has no key pair. A person reaches the host through Session Manager only; a gateway endpoint keeps the traffic to S3 inside the network of AWS.

**4. Roles with the smallest rights which the code needs, and no long-lived key.** The host has one role with policies split by duty: the role can write a raw file and cannot delete one. ClickHouse reads the lake through a second role which can read the Iceberg tables and nothing else. GitHub Actions assumes two roles through OIDC: one reads for `terraform plan`, one can send a single command document to the host. No access key exists.

**5. Secrets live in Parameter Store, not in Terraform.** The host reads its secrets from encrypted entries below `/gbmi/prod/` at each start and each deployment. Terraform manages no entry, so no secret stands in the Terraform state.

**6. A person applies Terraform; a workflow deploys code.** Terraform describes every AWS resource. A pull request shows the plan through the read-only role. A person reads the plan and applies it on the laptop; no workflow can apply. A deployment is one command document with one parameter, a commit of `main`; the script on the host checks the commit and the image before it changes anything.

**7. Cost controls.**

| Control | What it does |
|---|---|
| A monthly budget of 30 US dollars | Notifications at 50, 80 and 100 percent |
| The nightly stop | A third of the instance hours is not bought |
| The spot market | 0.0227 US dollars an hour in the zone of the host on 2026-11-28, against 0.0672 on demand: about a third. The spot price changes |
| No NAT gateway, no load balancer, no managed database | The three largest fixed costs of a usual design are absent |
| Lifecycle rules of the buckets and a log retention of 30 days | Old versions, temporary files and logs expire by themselves |
| An alarm on surplus CPU credits and an alarm on a failed call of the schedule | A host which works above its baseline, and a host which does not stop, are reported |
| Tags `project` and `env` on every resource | The bill can be read by project |

## Alternatives considered

| Alternative | Why it lost |
|---|---|
| Amazon Managed Workflows for Apache Airflow | About 358 US dollars a month for the smallest environment (blueprint, TD-14), twelve times the budget, and it would not run ClickHouse or PostgreSQL |
| A container service with one task for each service (ECS on Fargate) | Each service needs a task which runs all day, a network with a NAT gateway or public tasks, and a managed database or a volume service. The cost and the number of parts both grow; one Compose file serves the laptop and the host instead |
| Kubernetes | Three to four long-running services on one machine do not need a scheduler of containers (TD-15) |
| An on-demand instance | About 1.08 US dollars for a day of 16 hours (blueprint §12.1): about 32 US dollars a month for the instance alone. On-demand stays the fallback for the first apply when no spot capacity exists |
| An instance which runs all day | Half as much again for hours in which nobody reads the marts. The pipelines catch up from the ledger after each start |
| A private subnet with a NAT gateway | About 33 US dollars a month. The public address is acceptable because no inbound rule exists and Session Manager needs none |
| SSH with a key pair | A key is a long-lived secret, and port 22 is an inbound rule. Session Manager needs neither and records each session |
| A managed PostgreSQL for the operational database | A further fixed cost each month and a further service to operate, for a database of a few megabytes. Its price was not looked up. The price of the decision is a database without a managed backup; phase P4 adds the backup |
| Secrets in Terraform variables or in a file on the host | A Terraform variable ends in the state file. A file on the host has to get there, and a new host would start without it |
| A workflow which applies Terraform | The workflow would need a role which can create roles. With one developer, the review of a plan by a person is the control |
| A cheaper virtual machine of another provider | Cheaper, and without the services of AWS which the project sets out to use (TD-14) |

## Consequences

- **One machine is a single point of failure, by design.** A lost host means a lost operational database until phase P4 adds a backup. Three guards lower the risk: Terraform refuses to destroy or replace the instance, the instance ignores a newer image and a changed start script, and the disk outlives the instance.
- **The platform is down for 8 hours each night.** Data which arrives in the night is loaded after the start: the ledger knows which files are missing.
- **A start can fail.** AWS starts a stopped spot instance only when spot capacity is free. The schedule tries a failed start again for 12 hours, and an alarm reports the first failed call. The market, the zone and the type of the host are fixed when the host is created.
- **The host is on the public internet.** The protection is the missing inbound rule, the missing key pair, version 2 of the metadata service, and one outbound rule.
- **A container can use the role of the host.** The metadata service allows two network hops, so every container of the stack receives the rights of the host role. The split policies limit what that is. The two AWS policies for the agents allow more than the split; phase P4 replaces them.
- **A change of the first-boot script cannot reach a running host.** The steps of a deployment therefore live in the release, and a rehearsal in a container (`make host-rehearsal`) tests the first boot before an instance exists.
- **A person has to be there for an apply.** No infrastructure change happens without a sign-in of the root user of the account, which is the access model of week 1 (D13).

## Evidence

| Claim | Evidence |
|---|---|
| The code builds what the record describes | `make tf-check`: format, validation, tflint and 322 policy checks of checkov; each skipped check has a written reason. Review 3 of 2026-11-28 found no blocking defect; its three defects are repaired |
| The first boot and the deployments work | `make host-rehearsal`: the start script of the instance, three deployments, each refusal and the start at boot in a container of Amazon Linux 2023 for arm64: 80 checks |
| A pull request can plan and cannot write | The job `plan` of pull request #28 assumed the read-only role through OIDC on 2026-11-28 and planned the configuration `bootstrap` |
| The applied resources match the code | Both configurations were applied on 2026-11-28 from saved plans, each after the answer "apply" of the developer; a plan of each configuration reports "No changes" afterwards |
| The host has no way in | Read back from AWS after the apply: the security group has one outbound rule and no inbound rule, the instance has no key pair, and the metadata service accepts version 2 only |
| The roles allow what the code needs and refuse the rest | `make host-policy-check` after the applies: IAM decided 86 of 86 simulated requests against the five roles as expected. The host role cannot delete a raw file; the plan role cannot read a secret of the host |
| No secret in the Terraform state | A search of both state files for the values of the 15 parameters of the host found none |
| The host is reachable only through Session Manager; the stack runs on S3 and Glue | **Open.** Exit-gate rows 4 and 5 of week 3 |
| The stack fits 8 GB; the CPU credits and the memory under load | **Open.** The measurements of work package WP-13 (exit-gate row 12) |
| The platform costs about 30 US dollars a month | **Open until phase P4.** The numbers of the record are estimates of the blueprint. The cost report of P4 reads the bill |

## Status

**Provisional.** The network, the roles, the host and the monitoring are applied since 2026-11-28. The record is updated after the measurements of the host, and it becomes *accepted* with the cost report of phase P4.
