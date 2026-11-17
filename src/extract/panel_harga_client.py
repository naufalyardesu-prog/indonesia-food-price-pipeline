"""Client sederhana untuk API Panel Harga Badan Pangan."""
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

BASE_URL = "https://api-panelhargav2.badanpangan.go.id/api/front"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
TIMEOUT_DETIK = (10, 60)  # (tunggu koneksi, tunggu jawaban)
JEDA_DETIK = 3
LEVEL_HARGA = {1: "produsen", 2: "grosir", 3: "konsumen"}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=5, max=20), reraise=True)
def ambil_json(endpoint: str, params: dict) -> dict:
    """Panggil satu endpoint API, lalu kembalikan isinya sebagai dict."""
    response = requests.get(
        f"{BASE_URL}/{endpoint}", params=params, headers=HEADERS, timeout=TIMEOUT_DETIK
    )
    response.raise_for_status()
    data = response.json()
    if data.get("status") != "success":
        raise ValueError(f"API tidak sukses: {data.get('message')}")
    return data


def ambil_harga_provinsi(tanggal: str, level_harga_id: int, komoditas_id: int) -> dict:
    """Ambil harga satu komoditas di semua provinsi untuk satu tanggal (format YYYY-MM-DD)."""
    tgl = datetime.strptime(tanggal, "%Y-%m-%d").strftime("%d/%m/%Y")
    params = {
        "level_harga_id": level_harga_id,
        "komoditas_id": komoditas_id,
        "period_date": f"{tgl} - {tgl}",
        "multi_status_map[0]": "",
        "multi_province_id[0]": "",
    }
    return ambil_json("harga-peta-provinsi", params)


def folder_bronze(tanggal: str, folder_data: str) -> Path:
    """Alamat folder bronze untuk satu tanggal."""
    return Path(folder_data) / "bronze" / f"date={tanggal}"


def simpan_bronze(data: dict, tanggal: str, nama_file: str,
                  folder_data: str = "/opt/airflow/data") -> Path:
    """Simpan JSON mentah ke data/bronze/date=YYYY-MM-DD/nama_file."""
    folder = folder_bronze(tanggal, folder_data)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / nama_file
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return path


def dapatkan_daftar_komoditas(level_id: int, tanggal: str, folder_data: str) -> dict:
    """Ambil daftar komoditas dari API. Kalau gagal, pakai file yang sudah ada di bronze."""
    nama_file = f"komoditas_level_{level_id}.json"
    cadangan = folder_bronze(tanggal, folder_data) / nama_file
    try:
        params = {"province_id": "", "city_id": "", "level_harga_id": level_id}
        daftar = ambil_json("harga-pangan-informasi", params)
        simpan_bronze(daftar, tanggal, nama_file, folder_data)
        return daftar
    except Exception as error:
        if cadangan.exists():
            logger.warning("Daftar komoditas level %s gagal diambil (%s), pakai file cadangan",
                           level_id, error)
            return json.loads(cadangan.read_text(encoding="utf-8"))
        raise


def ekstrak_satu_tanggal(tanggal: str, folder_data: str = "/opt/airflow/data") -> dict:
    """Ambil harga semua komoditas di ketiga level untuk satu tanggal, simpan ke bronze."""
    ringkasan = {"berhasil": 0, "dilewati": 0, "gagal": []}

    for level_id, nama_level in LEVEL_HARGA.items():
        daftar = dapatkan_daftar_komoditas(level_id, tanggal, folder_data)
        time.sleep(JEDA_DETIK)

        for komoditas in daftar["data"]:
            nama_file = f"level_{level_id}_komoditas_{komoditas['id']}.json"

            if (folder_bronze(tanggal, folder_data) / nama_file).exists():
                ringkasan["dilewati"] += 1
                logger.info("%s | %s | %s: sudah ada, dilewati",
                            tanggal, nama_level, komoditas["name"])
                continue

            try:
                data = ambil_harga_provinsi(tanggal, level_id, komoditas["id"])
                simpan_bronze(data, tanggal, nama_file, folder_data)
                ringkasan["berhasil"] += 1
                logger.info("%s | %s | %s: %d provinsi",
                            tanggal, nama_level, komoditas["name"], len(data["data"]))
            except Exception as error:
                ringkasan["gagal"].append(nama_file)
                logger.warning("Gagal %s: %s", nama_file, error)

            time.sleep(JEDA_DETIK)

    return ringkasan
