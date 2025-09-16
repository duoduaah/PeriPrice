.PHONY: migrate

migrate:
	chmod +x infra/bigquery/run_migration.sh
	./infra/bigquery/run_migration.sh