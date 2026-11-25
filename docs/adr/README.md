# Architecture decision records

One record per decision that shapes the platform. The blueprint (§10) lists the fourteen that are planned.

## Template

Every record has the same six sections (blueprint §15.4):

1. **Context** — the forces at play and what has to be decided.
2. **Decision** — what was chosen, stated so that it can be checked.
3. **Alternatives considered** — what else was on the table and why it lost.
4. **Consequences** — what becomes easier, what becomes harder, what is given up.
5. **Evidence** — the measurement or test that backs the decision.
6. **Status** — *proposed*, *provisional* (decided, but evidence is missing or partial), *accepted*, or *superseded by ADR-nnn*.

A decision without evidence is marked provisional.

## Index

| ADR | Subject | Status |
|---|---|---|
| [001](001-scope-non-goals-success-criteria.md) | Scope, non-goals and success criteria | Accepted |
| [004](004-iceberg-glue-local-backend.md) | Iceberg with Glue; a local filesystem backend instead of an S3 emulator | Provisional |
| [006](006-incremental-contract-partition-replacement.md) | Incremental contract: partition replacement | Provisional (draft) |
