import csv
import datetime
import os
import re
from typing import Iterable, List, Tuple

import pandas as pd
import xarray as xr

import config

DOWNLOAD_DIR = config.DOWNLOAD_DIR
MODEL_NAME = config.MODEL_NAME
BWR_AREAS = config.BWR_AREAS
BWR_LEVELS_MB = config.BWR_LEVELS_MB
BWR_OUTPUT_DIR = config.BWR_OUTPUT_DIR


FILENAME_PATTERN = re.compile(r"BWR_all_f(?P<hour>\d{3})\.grib$")


class GribParsingError(RuntimeError):
    """Sygnalizuje problem z parsowaniem plików GRIB dla meldunków BWR."""


def _find_grib_files(directory: str) -> List[str]:
    files = []
    if os.path.isdir(directory):
        for name in os.listdir(directory):
            if FILENAME_PATTERN.match(name):
                files.append(os.path.join(directory, name))
    return sorted(files)


def _forecast_hour_from_filename(path: str) -> int:
    name = os.path.basename(path)
    match = FILENAME_PATTERN.match(name)
    if not match:
        raise GribParsingError(f"Niepoprawna nazwa pliku GRIB: {name}")
    return int(match.group("hour"))


def _extract_cycle(ds: xr.Dataset) -> datetime.datetime:
    if "time" not in ds:
        raise GribParsingError("Brak współrzędnej 'time' w pliku GRIB.")
    try:
        cycle_ts = pd.to_datetime(ds["time"].item())
    except Exception as exc:  # noqa: BLE001
        raise GribParsingError(f"Nie można odczytać czasu cyklu z pliku GRIB: {exc}") from exc
    return cycle_ts.to_pydatetime()


def _extract_valid_time(ds: xr.Dataset, fallback: datetime.datetime, step_hours: int) -> datetime.datetime:
    if "valid_time" in ds:
        try:
            valid_ts = pd.to_datetime(ds["valid_time"].item())
            return valid_ts.to_pydatetime()
        except Exception:  # noqa: BLE001
            pass
    return fallback + datetime.timedelta(hours=step_hours)


def _open_dataset(grib_path: str) -> xr.Dataset:
    try:
        return xr.open_dataset(grib_path, engine="cfgrib")
    except Exception as exc:  # noqa: BLE001
        raise GribParsingError(f"Błąd podczas otwierania pliku GRIB {grib_path}: {exc}") from exc


def _extract_point_wind(ds: xr.Dataset, lat: float, lon: float, level: int) -> Tuple[float, float]:
    if "u" not in ds or "v" not in ds:
        raise GribParsingError("Plik GRIB nie zawiera komponentów wiatru 'u' i 'v'.")
    try:
        point = ds.sel(latitude=lat, longitude=lon, method="nearest")
        u_val = float(point["u"].sel(isobaricInhPa=level).item())
        v_val = float(point["v"].sel(isobaricInhPa=level).item())
    except KeyError as exc:
        raise GribParsingError(f"Brak poziomu {level} hPa w pliku GRIB.") from exc
    except Exception as exc:  # noqa: BLE001
        raise GribParsingError(f"Błąd podczas odczytu wiatru dla poziomu {level} hPa: {exc}") from exc
    return round(u_val, 5), round(v_val, 5)


def parse_bwr_to_csv(
    grib_directory: str = DOWNLOAD_DIR,␊
    output_csv: str = os.path.join(BWR_OUTPUT_DIR, "bwr_output.csv"),
    areas: Iterable[Tuple[str, float, float, float, float, float, float]] = BWR_AREAS,␊
) -> str:
    """
    Parsuje pliki GRIB BWR i zapisuje wiatry dla punktów środkowych obszarów do pliku CSV.

    Parametry:
        grib_directory: katalog z plikami BWR_all_fXXX.grib
        output_csv: ścieżka do wynikowego pliku CSV
        areas: lista obszarów z config.BWR_AREAS

    Zwraca:
        Ścieżkę do utworzonego pliku CSV.
    """
    grib_files = _find_grib_files(grib_directory)
    if not grib_files:
        raise GribParsingError(f"Brak plików GRIB w katalogu: {grib_directory}")

    first_ds = _open_dataset(grib_files[0])
    cycle_dt = _extract_cycle(first_ds)
    cycle_str = cycle_dt.strftime("%Y%m%d_%H")

    columns = [
        "bwr_name",
        "yyyy",
        "mm",
        "dd",
        "hh",
        "lat",
        "lon",
    ]
    for level in BWR_LEVELS_MB:
        columns.extend([f"u{level}", f"v{level}"])

    rows: List[List[str]] = []

    for grib_path in grib_files:
        forecast_hour = _forecast_hour_from_filename(grib_path)
        ds = _open_dataset(grib_path)
        valid_dt = _extract_valid_time(ds, cycle_dt, forecast_hour)

        for area in areas:
            name, center_lat, center_lon = area[:3]

            row = [
                name,
                valid_dt.year,
                f"{valid_dt.month:02d}",
                f"{valid_dt.day:02d}",
                f"{valid_dt.hour:02d}",
                center_lat,
                center_lon,
            ]

            for level in BWR_LEVELS_MB:
                u, v = _extract_point_wind(ds, center_lat, center_lon, level)
                row.extend([u, v])

            rows.append(row)

    os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([f"model={MODEL_NAME}", f"cycle={cycle_str}"])
        writer.writerow(columns)
        writer.writerows(rows)

    return output_csv


if __name__ == "__main__":
    try:
        output_file = parse_bwr_to_csv()
        print(f"Zapisano plik CSV: {output_file}")
    except GribParsingError as exc:
        print(f"[ERROR] {exc}")
    except FileNotFoundError as exc:
        print(f"[ERROR] Brak pliku: {exc}")