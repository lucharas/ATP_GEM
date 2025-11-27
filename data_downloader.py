import datetime
import os
from urllib.parse import urlencode

import requests
import config

# Wczytanie stałych z modułu config
BASE_URL = config.BASE_URL
DOWNLOAD_TIMEOUT = config.DOWNLOAD_TIMEOUT
DOWNLOAD_DIR = config.DOWNLOAD_DIR

# CDR
FCT_HOURS_CDR = config.FCT_HOURS_CDR
BBOX_CDR = config.BBOX_CDR
CDR_BLOCKS = config.CDR_BLOCKS
CDR_URL_MAP = config.CDR_URL_MAP

# BWR
FCT_HOURS_BWR = config.FCT_HOURS_BWR
BBOX_BWR = config.BBOX_BWR
BWR_VARS = config.BWR_VARS
BWR_LEVELS_MB_URL = config.BWR_LEVELS_MB_URL


def generate_gfs_urls(start_modelu_dt: datetime.datetime, is_cdr: bool, is_bwr: bool) -> dict:
    grouped_urls = {}

    date_str = start_modelu_dt.strftime("%Y%m%d")
    run_str = start_modelu_dt.strftime("%H")
    folder = f"/gfs.{date_str}/{run_str}/atmos"

    def build_common_params(bbox: dict) -> list:
        return [
            ("subregion", ""),
            ("leftlon", bbox["leftlon"]),
            ("rightlon", bbox["rightlon"]),
            ("toplat", bbox["toplat"]),
            ("bottomlat", bbox["bottomlat"]),
            ("dir", folder),
        ]

    # --- B. Generowanie URL dla BWR (blok: bwr_all) ---
    if is_bwr:
        bwr_urls = {}
        for fct_hour in FCT_HOURS_BWR:
            fct_str = f"f{fct_hour:03d}"

            filename = f"BWR_all_{fct_str}.grib"
            params = [
                ("file", f"gfs.t{run_str}z.pgrb2.0p25.{fct_str}"),
                *build_common_params(BBOX_BWR),
                *[(f"var_{field}", "on") for field in BWR_VARS],
                *((level_mb_url, "on") for level_mb_url in BWR_LEVELS_MB_URL),
            ]

            url = f"{BASE_URL}?{urlencode(params)}"
            bwr_urls[filename] = url

        grouped_urls["bwr_all"] = bwr_urls

    # --- A. Generowanie URL dla CDR (bloki wg poziomów) ---
    if is_cdr:
        for block_name, levels_list in CDR_BLOCKS.items():
            cdr_block_urls = {}

            level = levels_list[0]
            fields_on_level = CDR_URL_MAP.get(level, [])

            if not fields_on_level:
                continue

            for fct_hour in FCT_HOURS_CDR:
                fct_str = f"f{fct_hour:03d}"
                filename = f"CDR_{block_name}_{fct_str}.grib"

                params = [
                    ("file", f"gfs.t{run_str}z.pgrb2.0p25.{fct_str}"),
                    *build_common_params(BBOX_CDR),
                    *[(f"var_{field}", "on") for field in fields_on_level],
                    (level, "on"),
                ]

                url = f"{BASE_URL}?{urlencode(params)}"
                cdr_block_urls[filename] = url

            grouped_urls[f"cdr_{block_name}"] = cdr_block_urls

    return grouped_urls


def download_grib_files(grouped_urls: dict, log_callback, download_dir=DOWNLOAD_DIR) -> bool:
    """
    Pobiera pliki GRIB w blokach i raportuje status do GUI poprzez log_callback.
    """
    if not grouped_urls:
        log_callback("[WARN] Brak URL-i do pobrania.")
        return False

    os.makedirs(download_dir, exist_ok=True)
    total_files = sum(len(urls) for urls in grouped_urls.values())
    files_downloaded = 0
    all_success = True

    for block_name, files_map in grouped_urls.items():
        log_callback(f"[INFO] Rozpoczęto blok: {block_name} ({len(files_map)} plików)...")

        for filename, url in files_map.items():
            file_path = os.path.join(download_dir, filename)

            try:
                headers = {'User-Agent': 'ATP_Model_Downloader_v1.0'}

                response = requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT, headers=headers)
                response.raise_for_status()

                if 'Content-Length' in response.headers and int(response.headers['Content-Length']) < 1000:
                    log_callback(f"[ERROR] Plik {filename} jest za mały. Prawdopodobnie błąd NOMADS/pusta odpowiedź.")
                    all_success = False
                    files_downloaded += 1
                    continue

                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                files_downloaded += 1
                log_callback(f"[PROGRESS] Pobrano: {filename} ({files_downloaded}/{total_files})")

            except requests.exceptions.RequestException as e:
                log_callback(f"[FATAL] Błąd pobierania {filename}: {e}")
                all_success = False
                files_downloaded += 1

    log_callback(f"[INFO] Zakończono pobieranie. Pobrano {files_downloaded} z {total_files} plików.")
    return all_success
