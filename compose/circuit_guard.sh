#!/usr/bin/env bash
# make reset for the working stack: refuse while a block by the SEC is recorded.
#
# The row of ops.source_circuit in the working database remembers a block. A reset deletes
# the database, and the next client would then ask the SEC during the block (review 1 of
# week 3, finding B2). The script starts PostgreSQL of the stack, reads the row and stops the
# reset when a block or a lock is recorded.
set -euo pipefail

STACK="${STACK:?STACK is not set: run the script through make}"
COMPOSE=(docker compose --project-name "$STACK" --project-directory . --env-file .env -f compose/compose.yaml)

# Without the volume of PostgreSQL the stack has no database, so no block is recorded.
if ! docker volume inspect "${STACK}_postgres-data" >/dev/null 2>&1; then
  exit 0
fi

"${COMPOSE[@]}" up --detach --wait postgres >/dev/null 2>&1 || {
  echo "circuit guard: PostgreSQL of the stack $STACK did not start, so the circuit row cannot be read."
  echo "Start the stack with make up, then run make reset again."
  exit 1
}
query() {
  "${COMPOSE[@]}" exec -T postgres psql -U postgres -d gbmi -At -c "$1"
}
if [ "$(query "SELECT to_regclass('ops.source_circuit') IS NOT NULL")" != "t" ]; then
  exit 0
fi
recorded="$(query "SELECT count(*) FROM ops.source_circuit WHERE blocked_until IS NOT NULL")"
if [ "$recorded" != "0" ]; then
  echo "A block by the SEC is recorded in the database of the stack $STACK."
  echo "make reset would delete the record, and the next client would ask the SEC during the block."
  echo "Read the state:   make toolbox CMD=\"gbmi sec circuit status\""
  echo "After the cause is found and the block has ended:"
  echo "                  make toolbox CMD=\"gbmi sec circuit close --note '<what was found>'\""
  exit 1
fi
