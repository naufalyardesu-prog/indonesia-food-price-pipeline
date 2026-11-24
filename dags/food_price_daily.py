"""DAG harian: ambil harga pangan dari API Panel Harga, bersihkan dengan Spark, muat ke warehouse.

Run pukul 06.00 WIB memproses data kemarin (awal data interval, dalam WIB):
extract_prices -> transform_silver -> load_staging
"""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

TZ = "Asia/Jakarta"

default_args = {
    "owner": "yardan",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def tanggal_wib(data_interval_start) -> str:
    """Tanggal data dalam WIB, format YYYY-MM-DD. Airflow menyimpan waktu dalam UTC."""
    return data_interval_start.in_timezone(TZ).to_date_string()


@dag(
    dag_id="food_price_daily",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["food-price", "batch"],
    doc_md=__doc__,
)
def food_price_daily():
    @task(execution_timeout=timedelta(minutes=30))
    def extract_prices(data_interval_start=None) -> dict:
        """Ambil harga semua komoditas untuk tanggal data dan simpan ke bronze."""
        from src.extract.panel_harga_client import ekstrak_satu_tanggal

        tanggal = tanggal_wib(data_interval_start)
        ringkasan = ekstrak_satu_tanggal(tanggal)
        if ringkasan["gagal"]:
            raise ValueError(f"{len(ringkasan['gagal'])} file gagal diambil: {ringkasan['gagal']}")
        return ringkasan

    @task(execution_timeout=timedelta(minutes=15))
    def transform_silver(data_interval_start=None) -> int:
        """Ubah JSON bronze menjadi Parquet silver dengan Spark."""
        from src.transform.bronze_to_silver import transform_tanggal

        tanggal = tanggal_wib(data_interval_start)
        jumlah = transform_tanggal(tanggal)
        if jumlah == 0:
            raise ValueError(f"Data silver untuk {tanggal} kosong")
        return jumlah

    @task(execution_timeout=timedelta(minutes=10))
    def load_staging(data_interval_start=None) -> int:
        """Hapus lalu isi ulang data tanggal tersebut di staging.stg_daily_price."""
        from src.load.staging_loader import muat_tanggal

        return muat_tanggal(tanggal_wib(data_interval_start))

    extract_prices() >> transform_silver() >> load_staging()


food_price_daily()
