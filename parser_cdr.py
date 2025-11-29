import csv
import datetime
import os
import re
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import xarray as xr

import config

DOWNLOAD_DIR = config.DOWNLOAD_DIR
MODEL_NAME = config.MODEL_NAME
CDR_AREAS = config.CDR_AREAS
FIELDS_CDR = config.FIELDS_CDR
CDR_OUTPUT_DIR = config.CDR_OUTPUT_DIR

CDR_FILENAME_PATTERN = re.compile(
    r"CDR_(?P<block>[a-zA-Z0-9_]+)_f(?P<hour>\d{3})\.grib$"
)

LEVEL_TO_BLOCK = {
    "lev_surface": "sfc",
    "lev_2_m_above_ground": "2m",
    "lev_10_m_above_ground": "10m",
    "lev_80_m_above_ground": "80m",
    "lev_entire_atmosphere": "entire_atmosphere",
}

CDR_FIELDS_ORDER = [
    "u10",
    "v10",
    "u80",
    "v80",
    "t2",
    "t80",
    "q2",
    "q80",
    "rh2",
    "vis",
    "tcc",
    "apcp",
    "acpcp",
    "cape",
]

VAR_ALIASES: Dict[str, List[str]] = {
    "u10": ["10u", "u"],
    "v10": ["10v", "v"],
    "u80": ["80u", "u"],
    "v80": ["80v", "v"],
    "t2": ["2t", "t"],
    "t80": ["80t", "t"],
    "q2": ["2q", "q"],
    "q80": ["80q", "q"],
    "rh2": ["2r", "r"],
    "vis": ["vis"],
    "tcc": ["tcc"],
    "apcp": ["tp", "apcp"],
    "acpcp": ["acpcp"],
    "cape": ["cape"],
}


class GribParsingError(RuntimeError):
    """Sygnalizuje problem z parsowaniem plików GRIB dla meldunków CDR."""


def _expected_short_name(var: str, level: str) -> str:
    mapping = {
        ("UGRD", "lev_10_m_above_ground"): "10u",
        ("VGRD", "lev_10_m_above_ground"): "10v",
        ("UGRD", "lev_80_m_above_ground"): "80u",
        ("VGRD", "lev_80_m_above_ground"): "80v",
        ("TMP", "lev_2_m_above_ground"): "2t",
        ("TMP", "lev_80_m_above_ground"): "80t",
        ("SPFH", "lev_2_m_above_ground"): "2q",
        ("SPFH", "lev_80_m_above_ground"): "80q",
        ("RH", "lev_2_m_above_ground"): "2r",
        ("VIS", "lev_surface"): "vis",
        ("TCDC", "lev_entire_atmosphere"): "tcc",
        ("APCP", "lev_surface"): "tp",
        ("ACPCP", "lev_surface"): "acpcp",
        ("CAPE", "lev_surface"): "cape",
    }
    return mapping.get((var, level), "")


def _find_grib_files(directory: str) -> Dict[int, Dict[str, str]]:
    files: Dict[int, Dict[str, str]] = {}
    if not os.path.isdir(directory):
        return files

    for name in os.listdir(directory):
        match = CDR_FILENAME_PATTERN.match(name)
        if not match:
            continue

        hour = int(match.group("hour"))
        block = match.group("block")
        files.setdefault(hour, {})[block] = os.path.join(directory, name)

    return files


def _forecast_hour_from_filename(path: str) -> int:
    name = os.path.basename(path)
    match = CDR_FILENAME_PATTERN.match(name)
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


def _select_dataarray(ds: xr.Dataset, field_name: str, var: str, level: str) -> xr.DataArray:
    short_name = _expected_short_name(var, level)

    for da_name, dataarray in ds.data_vars.items():
        if dataarray.attrs.get("GRIB_shortName") == short_name:
            return dataarray.squeeze()

    if field_name in ds:
        return ds[field_name].squeeze()

    for alias in VAR_ALIASES.get(field_name, []):
        if alias in ds:
            return ds[alias].squeeze()

    raise GribParsingError(f"Brak pola {field_name} w pliku GRIB.")


def _coord_slice(min_val: float, max_val: float, coord_values) -> slice:
    values = list(coord_values)
    if len(values) >= 2 and values[1] < values[0]:
        return slice(max_val, min_val)
    return slice(min_val, max_val)


def _build_field_metadata() -> Dict[str, Tuple[str, str, str]]:
    metadata: Dict[str, Tuple[str, str, str]] = {}
    for var, lev, name in FIELDS_CDR:
        metadata[name] = (var, lev, LEVEL_TO_BLOCK.get(lev, ""))
    return metadata


FIELD_METADATA = _build_field_metadata()


def parse_cdr_to_csv(
    grib_directory: str = DOWNLOAD_DIR,
    output_csv: str = os.path.join(CDR_OUTPUT_DIR, "cdr_output.csv"),
    areas: Iterable[Tuple[str, float, float, float, float, float, float]] = CDR_AREAS,
) -> str:
    grib_map = _find_grib_files(grib_directory)
    if not grib_map:
        raise GribParsingError(f"Brak plików GRIB w katalogu: {grib_directory}")

    required_blocks = set(LEVEL_TO_BLOCK.values())

    first_hour_blocks = next(iter(grib_map.values()))
    first_ds = _open_dataset(next(iter(first_hour_blocks.values())))
    cycle_dt = _extract_cycle(first_ds)
    cycle_str = cycle_dt.strftime("%Y%m%d_%H")

    columns = [
        "cdr_name",
        "yyyy",
        "mm",
        "dd",
        "hh",
        "lat",
        "lon",
        *CDR_FIELDS_ORDER,
    ]

    rows: List[List[str]] = []

    for hour in sorted(grib_map.keys()):
        block_paths = grib_map[hour]
        missing_blocks = required_blocks - set(block_paths)
        if missing_blocks:
            print(
                f"[WARN] Brak plików bloków CDR {sorted(missing_blocks)} "
                f"dla godziny f{hour:03d}; pomijam tę godzinę."
            )
            continue

        block_datasets: Dict[str, xr.Dataset] = {
            block: _open_dataset(path) for block, path in block_paths.items()
        }

        base_ds = block_datasets.get("10m") or next(iter(block_datasets.values()))
        valid_dt = _extract_valid_time(base_ds, cycle_dt, hour)

        base_lat_slice = _coord_slice(
            min(area[3] for area in areas),
            max(area[4] for area in areas),
            base_ds.latitude.values,
        )
        base_lon_slice = _coord_slice(
            min(area[5] for area in areas),
            max(area[6] for area in areas),
            base_ds.longitude.values,
        )

        base_subset = base_ds.sel(latitude=base_lat_slice, longitude=base_lon_slice)

        field_cache: Dict[Tuple[str, str], xr.DataArray] = {}

        for area in areas:
            name, _center_lat, _center_lon, min_lat, max_lat, min_lon, max_lon = area

            area_subset = base_subset.sel(
                latitude=_coord_slice(min_lat, max_lat, base_subset.latitude.values),
                longitude=_coord_slice(min_lon, max_lon, base_subset.longitude.values),
            )

            for lat_val in area_subset.latitude.values:
                for lon_val in area_subset.longitude.values:
                    row = [
                        name,
                        valid_dt.year,
                        f"{valid_dt.month:02d}",
                        f"{valid_dt.day:02d}",
                        f"{valid_dt.hour:02d}",
                        float(lat_val),
                        float(lon_val),
                    ]

                    for field_name in CDR_FIELDS_ORDER:
                        var, level, block = FIELD_METADATA[field_name]
                        cache_key = (block, field_name)
                        if cache_key not in field_cache:
                            ds = block_datasets[block]
                            field_cache[cache_key] = _select_dataarray(ds, field_name, var, level)

                        da = field_cache[cache_key]
                        try:
                            value = da.sel(latitude=lat_val, longitude=lon_val).item()
                        except Exception as exc:  # noqa: BLE001
                            raise GribParsingError(
                                f"Błąd odczytu pola {field_name} dla punktu ({lat_val}, {lon_val})."
                            ) from exc

                        row.append(round(float(value), 5))

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
        output_file = parse_cdr_to_csv()
        print(f"Zapisano plik CSV: {output_file}")
    except GribParsingError as exc:
        print(f"[ERROR] {exc}")
    except FileNotFoundError as exc:
        print(f"[ERROR] Brak pliku: {exc}")