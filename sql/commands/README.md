# PostgreSQL commands

Run the read-only customer verification from the repository root in Git Bash,
WSL, or Bash:

```bash
bash sql/commands/scripts/verify_customers.sh
```

The script executes `verify_customers.sql` in the running
`retailpulse-postgres` container using its `POSTGRES_USER` and `POSTGRES_DB`.
It prints the connected role, database, and customer row count. It stops on SQL
errors and does not create roles or modify data.

To open an interactive SQL session:

```bash
docker exec -it retailpulse-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Exit with `\q`.

## Create the reporting analyst role

First create `ai_reporting.customer_metrics` under a trusted owner with the
necessary underlying table permissions. The view should use the default owner
permission checks, rather than `security_invoker`.

Run the separate administrator script from PowerShell:

```powershell
docker cp sql/create_analyst_role.sql retailpulse-postgres:/tmp/create_analyst_role.sql
docker exec -it retailpulse-postgres sh -c 'psql -U "$POSTGRES_USER" -d retailpulse -f /tmp/create_analyst_role.sql'
```

The configured PostgreSQL user must have permission to create roles and grant
the listed privileges. Supply a strong password at the hidden `psql` password
prompts; do not store it in SQL or shell history. The script creates a new role
and fails if that role already exists.

The analyst receives access only to `ai_reporting.customer_metrics`, with
transactions read-only by default. Grant `SELECT` on each future approved view
explicitly; do not grant blanket object access or underlying schema access.
