"""Muat data silver (Parquet) ke tabel staging di PostgreSQL warehouse."""
import logging
from datetime import date, datetime
from pathlib import Path

import pyarrow.parquet as pq
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

KOLOM = [
    "date_key", "province_id", "province_name", "province_kode", "latitude", "longitude",
    "price_level_id", "commodity_id", "price_idr", "reference_price_min",
    "reference_price_max", "status_map",
]

DDL_TABEL_INDUK = """
CREATE TABLE IF NOT EXISTS staging.stg_daily_price (
    date_key            DATE        NOT NULL,
    province_id         INTEGER     NOT NULL,
    province_name       TEXT,
    province_kode       TEXT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    price_level_id      SMALLINT    NOT NULL,
    commodity_id        INTEGER     NOT NULL,
    price_idr           BIGINT,
    reference_price_min BIGINT,
    reference_price_max BIGINT,
    status_map          TEXT,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date_key, province_id, commodity_id, price_level_id)
) PARTITION BY RANGE (date_key);
"""


def siapkan_partisi(cur, tanggal: date) -> None:
    """Buat partisi bulanan, misalnya stg_daily_price_2026_11, kalau belum ada."""
    awal = tanggal.replace(day=1)
    akhir = date(awal.year + awal.month // 12, awal.month % 12 + 1, 1)
    nama = f"stg_daily_price_{awal:%Y_%m}"
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS staging.{nama} "
        "PARTITION OF staging.stg_daily_price FOR VALUES FROM (%s) TO (%s)",
        (awal, akhir),
    )


def muat_tanggal(tanggal: str, folder_data: str = "/opt/airflow/data",
                 conn_id: str = "warehouse") -> int:
    """Hapus lalu isi ulang data satu tanggal di staging. Mengembalikan jumlah baris."""
    folder = Path(folder_data) / "silver" / "daily_price" / f"date={tanggal}"
    baris = pq.read_table(folder, columns=KOLOM).to_pylist()
    nilai = [tuple(b[k] for k in KOLOM) for b in baris]
    tgl = datetime.strptime(tanggal, "%Y-%m-%d").date()

    conn = PostgresHook(postgres_conn_id=conn_id).get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(DDL_TABEL_INDUK)
            siapkan_partisi(cur, tgl)
            cur.execute("DELETE FROM staging.stg_daily_price WHERE date_key = %s", (tgl,))
            execute_values(
                cur,
                f"INSERT INTO staging.stg_daily_price ({', '.join(KOLOM)}) VALUES %s",
                nilai,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("%s: %d baris dimuat ke staging.stg_daily_price", tanggal, len(nilai))
    return len(nilai)
