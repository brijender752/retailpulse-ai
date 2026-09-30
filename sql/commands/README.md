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
