"""DAG uji coba: memastikan Airflow bisa menjalankan PySpark mode local."""
import pendulum
from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=["smoke-test"],
)
def smoke_test():
    @task
    def check_spark() -> int:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("smoke_test")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
        )
        row_count = spark.range(1000).count()
        spark.stop()
        print(f"Spark OK, menghitung {row_count} baris")
        return row_count

    check_spark()


smoke_test()
