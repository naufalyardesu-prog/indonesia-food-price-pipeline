"""DAG uji coba: memastikan Airflow bisa menulis dan membaca PostgreSQL warehouse."""
import pendulum
from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=["smoke-test", "warehouse"],
)
def warehouse_connection_test():
    @task
    def write_and_read() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="warehouse")
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS staging.healthcheck (
                checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                note       TEXT
            );
            INSERT INTO staging.healthcheck (note) VALUES ('tes koneksi dari Airflow');
            """
        )
        total = hook.get_first("SELECT count(*) FROM staging.healthcheck")[0]
        schemas = hook.get_records(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name IN ('staging', 'gold') ORDER BY 1"
        )
        print(f"Warehouse OK. Schema: {[s[0] for s in schemas]}, baris healthcheck: {total}")
        return total

    write_and_read()


warehouse_connection_test()
