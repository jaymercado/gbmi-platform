# Runbook: deploy a commit to the AWS host, and go back to the commit before

| | |
|---|---|
| **Applies to** | The AWS host of phase P2: one EC2 instance which runs the Compose stack (blueprint §9.2 and §9.7; week 3 plan §3.3) |
| **Written** | 2026-11-28, phase P2, work package WP-12, **before the first deployment** |
| **Proved by** | So far: `tests/unit/test_host_scripts.py` (the host scripts with stand-in commands for `aws` and `docker`), `tests/unit/test_compose_prod.py`, `tests/unit/test_host_secrets.py` and `make host-rehearsal` (the user data, three deployments, each refusal and the start at boot in a container of Amazon Linux 2023). The first deployment (WP-12) repeats every step on the host; the runbook is corrected then |

The words of the runbook:

- A **release** is the content of one commit of `main` on the host: the folder `/opt/gbmi/releases/<commit>/` and the image `ghcr.io/<owner>/gbmi-airflow:<commit>`.
- **`/opt/gbmi/DEPLOYED`** is one file with the commit which the host runs. The host starts the release of the file at every boot. A deployment writes the file only after its smoke test has passed.
- **`/opt/gbmi/PREVIOUS`** is one file with the commit which the host ran before the deployed one. The release folder of that commit stays on the disk. A second deployment of the commit which already runs leaves the file as it is. The first deployment of a host writes no such file.
- The **environment file** `/etc/gbmi/env` holds the secrets and the values of the host for Compose. The host writes the file from Parameter Store at every start and at every deployment. Only root can read the file.
- The **smoke test** is `compose/smoke.sh` in the mode `host`: twenty checks, the last of which lands and builds one live hour of GDELT with the tracer DAG.

Every command of the runbook runs on the laptop, in the repository folder, unless the text says "on the host". The commands for AWS need a sign-in: `aws login --remote --profile gbmi-login` (see `infra/terraform/README.md`).

## The rules

1. **Only a commit of `main` is deployed, and only through the workflow `deploy`.** The script of the host refuses a commit which is not part of `main` and a commit without an image in the registry.
2. **Jay approves each deployment in GitHub.** Nobody else approves, and nobody bypasses the approval.
3. **Database changes go forward only** (blueprint §9.7). A release never removes a migration. Going back to an older release keeps the newer tables and columns in the database.
4. **No deployment during a live period which is being measured** (exit-gate row 5 of week 3).
5. **The SEC source of the host is a separate switch.** A deployment never turns requests to the SEC on. See "The SEC source of the host".
6. **Never cancel the spot request of the host, and never terminate the instance, in the AWS console.** AWS terminates a stopped spot instance when its request is cancelled. The operational database lives on the disk of the instance and has no backup before phase P4.
7. **One deployment or one start at a time.** A deployment and the start at boot use one lock. A deployment which arrives while the host still starts its stack is refused with "another deployment or the start at boot is running": wait a few minutes and start the workflow again.

## Deploy a commit

1. The commit is merged to `main`, and the two workflows `e2e` and `images` have ended with success for the commit:

   ```bash
   gh run list --branch main --limit 6
   ```

2. The host is running, and its agent is online:

   ```bash
   make host-status
   ```

3. Start the workflow for the head of `main`:

   ```bash
   make deploy
   ```

   For another commit of `main`: `gh workflow run deploy.yml --ref main -f commit=<40 characters>`.

4. Jay opens the run in GitHub (Actions, workflow `deploy`) and approves the deployment to the environment `prod`.
5. Follow the run. The job asks the host for the status every 15 seconds, for 55 minutes at most, and ends with a summary: the commit and the status. The summary never shows output of the host.

   ```bash
   gh run watch "$(gh run list --workflow deploy.yml --limit 1 --json databaseId --jq '.[0].databaseId')"
   ```

## What the host does

The workflow sends the command document `gbmi-prod-deploy` with one parameter, the commit. On the host:

1. The fixed script of the host checks the commit: 40 hexadecimal characters, part of `main`, and an image of the commit in the registry. Then the script unpacks the commit into its release folder and starts `compose/host/deploy.sh` of the release.
2. `render_env.sh` writes the environment file from the parameters below `/gbmi/prod/`.
3. Compose pulls the images of the release.
4. Compose starts the stack. The one-shot services apply the migrations of the operational database, create the roles and users, and prepare Airflow.
5. The smoke test runs.
6. Only after a passed smoke test: `DEPLOYED` is written, and `PREVIOUS` names the release which ran until then. The release before stays on the disk; older releases and unused images are removed.

## Check after a deployment

```bash
# The state of the instance and of the agent, the deployed commit and the services.
make host-status

# The Airflow of the host on http://127.0.0.1:18080 of the laptop, until Ctrl-C.
make host-forward
```

The line `deployed:` of `make host-status` shows the commit, and the line `previous:` shows the commit of the release before. Every service of the list is "Up" and "healthy".

## A deployment failed

The run of the workflow ends with a status other than `Success`. `DEPLOYED` still names the release before.

| What the summary says | What to read | Likely cause |
|---|---|---|
| "Expected one running instance" | `make host-status` | The host is stopped (the schedule stops the host at 24:00 UTC), or the spot instance was interrupted |
| Status `Failed`, after less than a minute | `make host-deploy-log` | The commit is not part of `main`; the image of the commit is missing (the workflow `images` has not ended, or the image is private) or the registry did not answer: the log shows the answer of Docker; a parameter is missing; or the host still starts its stack after a boot (rule 7) |
| Status `Failed`, after several minutes | `make host-deploy-log`; the line with `FAIL` names the check | A service did not become healthy, or a check of the smoke test failed |
| Status `TimedOut`, or `InProgress` after 55 minutes | `make host-status`, then `make host-deploy-log` | The host is under memory pressure, or the tracer DAG of the smoke test did not end. The workflow stops asking after 55 minutes; the deployment can still end on the host |

Read the output of the newest deployment. Systems Manager keeps the first 24,000 characters; the host scripts print the steps of a deployment and no value of a secret.

```bash
make host-deploy-log
```

A failed deployment may leave the new release running without a passed smoke test. Decide at once: repair and deploy again, or go back.

## Go back to the release before

**The normal way** is a deployment of the earlier commit, with the approval of Jay. The line `previous:` of `make host-status` shows the commit of the release before:

```bash
gh workflow run deploy.yml --ref main -f commit=<the 40 characters of the earlier commit>
```

**At once**, when the stack of a failed deployment must not keep running: start the release which `DEPLOYED` names. The step needs no approval, because it starts what was deployed before.

```bash
make host-shell
# on the host:
cat /opt/gbmi/DEPLOYED
sudo /opt/gbmi/releases/"$(cat /opt/gbmi/DEPLOYED)"/compose/host/stack-up
```

The same happens at the next start of the host at 08:00 UTC.

Going back does not undo a migration (rule 3). When the failed release applied a migration, the older code runs on the newer schema. A migration only adds tables, columns and checks, so the older code keeps working in the cases which the tests of the migration cover; read the migration file before a decision.

## The host did not start in the morning

The schedule starts the host at 08:00 UTC. A start fails when AWS has no free spot capacity for the instance type in the zone of the host.

- **Signs:** an email of the alarm `gbmi-prod-host-schedule-failed`; `make host-status` shows the state `stopped`.
- **What happens by itself:** the schedule tries the start again for 12 hours, until 20:00 UTC. The alarm sends a second email when a call of the schedule works again.
- **Start by hand:** `make host-start` sends one start call and waits. The call fails with the same error while no capacity is free.
- **After the start:** the unit `gbmi-stack.service` starts the deployed release. The DAGs then build the hours which the host missed, from the ledger.
- **No capacity for more than a day:** the market of an existing host cannot be changed (see `infra/terraform/README.md`). A host on the on-demand market is a new instance, and the operational database has to move with its disk. No command of the repository does that: decide the steps with Jay first.

`make host-stop` stops the host by hand. The schedule starts the host again at 08:00 UTC.

## Secrets of the host

```bash
# Create each missing entry below /gbmi/prod/; an existing entry is never changed.
make host-secrets
```

The command prints names only. The host reads the entries at the next start or deployment. To change one secret, change the entry in Parameter Store by hand and deploy again; a changed database password also has to be changed inside the database, which no command of the repository does.

## The SEC source of the host

The host sends requests to the SEC only when the parameter `/gbmi/prod/GBMI_SEC_SOURCE` says `live`. The SEC counts requests by user, not by machine: **one place sends live requests**.

```bash
# 1. The laptop: the working stack has the SEC source off in the Makefile, and its circuit is closed.
make toolbox CMD="gbmi sec circuit status"

# 2. The host. The command refuses "live" while the Makefile of the laptop says otherwise.
make host-sec-source SOURCE=live

# 3. The value takes effect at the next start or deployment of the host.
```

`make host-sec-source SOURCE=off` turns the requests of the host off again. The runbook [the circuit of the SEC is open or locked](sec-circuit-open.md) applies to the host as to the laptop: run its commands on the host with `docker compose … run --rm toolbox gbmi sec circuit status`.

## Not covered

- A new host. The user data of the instance installs Docker, the fixed script and the unit which starts the stack; a replaced instance starts with an empty operational database. Phase P4 adds the backup of the database.
- A change of the user data or of the image of the instance: Terraform ignores both for an existing instance (week 3 plan §3.3).
