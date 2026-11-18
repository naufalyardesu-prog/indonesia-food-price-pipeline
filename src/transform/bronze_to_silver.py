"""Job Spark: ubah JSON mentah di bronze menjadi tabel harga harian di silver (Parquet)."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, LongType, StringType,
                               StructField, StructType)

# Struktur file harga-peta-provinsi (hanya kolom yang dipakai)
SCHEMA_BRONZE = StructType([
    StructField("data", ArrayType(StructType([
        StructField("date", StringType()),
        StructField("province_id", LongType()),
        StructField("province_name", StringType()),
        StructField("province_kode", StringType()),
        StructField("latlong", StringType()),
        StructField("level_harga", LongType()),
        StructField("commodity_id", StringType()),
        StructField("rata_rata_geometrik", DoubleType()),
        StructField("hpp_hap", StringType()),
        StructField("status_map", StringType()),
    ]))),
])

STATUS_VALID = ["Aman", "Waspada", "Intervensi"]


def buat_spark() -> SparkSession:
    """Spark mode local, memori dibatasi supaya muat di laptop 8 GB."""
    return (
        SparkSession.builder.master("local[2]")
        .appName("bronze_to_silver")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "Asia/Jakarta")
        .getOrCreate()
    )


def bersihkan(df_bronze: DataFrame) -> DataFrame:
    """Flatten JSON dan bersihkan kolom sesuai hasil data profiling."""
    baris = df_bronze.select(F.explode("data").alias("r")).select("r.*")

    # "40.000 - 57.000" -> ["40000", "57000"]; "12.000" -> ["12000"]
    acuan = F.split(F.regexp_replace("hpp_hap", r"\.", ""), "-")
    acuan_min = F.trim(acuan.getItem(0)).cast("long")
    acuan_max = F.trim(acuan.getItem(1)).cast("long")

    return (
        baris.select(
            F.to_date("date").alias("date_key"),
            F.col("province_id"),
            F.col("province_name"),
            F.col("province_kode"),
            F.split("latlong", ",").getItem(0).cast("double").alias("latitude"),
            F.split("latlong", ",").getItem(1).cast("double").alias("longitude"),
            F.col("level_harga").alias("price_level_id"),
            F.col("commodity_id").cast("long").alias("commodity_id"),
            F.when(F.col("rata_rata_geometrik") > 0, F.round("rata_rata_geometrik").cast("long"))
             .alias("price_idr"),
            acuan_min.alias("reference_price_min"),
            F.coalesce(acuan_max, acuan_min).alias("reference_price_max"),
            F.when(F.col("status_map").isin(STATUS_VALID), F.col("status_map"))
             .alias("status_map"),
        )
        .dropDuplicates(["date_key", "province_id", "commodity_id", "price_level_id"])
    )


def transform_tanggal(tanggal: str, folder_data: str = "/opt/airflow/data") -> int:
    """Proses satu tanggal: bronze -> silver. Mengembalikan jumlah baris yang ditulis."""
    sumber = f"{folder_data}/bronze/date={tanggal}/level_*_komoditas_*.json"
    tujuan = f"{folder_data}/silver/daily_price/date={tanggal}"

    spark = buat_spark()
    df_bronze = spark.read.schema(SCHEMA_BRONZE).option("multiLine", True).json(sumber)
    df_silver = bersihkan(df_bronze)

    df_silver.coalesce(1).write.mode("overwrite").parquet(tujuan)
    jumlah = spark.read.parquet(tujuan).count()
    spark.stop()
    return jumlah
