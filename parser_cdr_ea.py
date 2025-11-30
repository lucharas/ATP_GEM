import os
import warnings
import xarray as xr
import pandas as pd
import config  # Import ustawień

# Tłumienie ostrzeżeń
warnings.filterwarnings("ignore", category=FutureWarning)

# =======================
# KONFIGURACJA
# =======================
INPUT_DIR = config.DOWNLOAD_DIR  # "grib_data"
# Zapisujemy w katalogu cdr_output
OUTPUT_DIR = os.path.join(config.RAW_DATA_DIR)
OUTPUT_FILENAME = "cdr_ea.csv"

# Mapowanie zmiennych
VAR_SHORTNAME = 'tcc'   # Nazwa w GRIB
VAR_CSV_NAME = 'tcc'    # Nazwa w CSV

GRIB_FILTERS = {
    'shortName': VAR_SHORTNAME,
    'stepType': 'instant'
}

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    all_dfs = []  # Lista do trzymania ramek danych z każdej godziny
    cycle_header_str = None # Tutaj zapiszemy string z datą cyklu

    print(f"--- Start parsowania zbiorczego CDR EA ---")
    
    # Iteracja po godzinach prognozy zdefiniowanych w config.py
    for hour in config.FCT_HOURS_CDR:
        fct_str = f"{hour:03d}"
        filename_in = f"CDR_entire_atmosphere_f{fct_str}.grib"
        path_in = os.path.join(INPUT_DIR, filename_in)

        if not os.path.exists(path_in):
            print(f"[SKIP] Brak pliku: {filename_in}")
            continue

        try:
            # 1. Otwarcie Datasetu
            ds = xr.open_dataset(
                path_in,
                engine='cfgrib',
                backend_kwargs={'filter_by_keys': GRIB_FILTERS}
            )

            # Pobranie daty cyklu (runu modelu) z pierwszego poprawnego pliku
            if cycle_header_str is None:
                # ds.time to data uruchomienia modelu (Cycle)
                # Konwersja numpy.datetime64 na string w formacie YYYYMMDD_HH
                run_time = pd.to_datetime(ds.time.values)
                cycle_str = run_time.strftime('%Y%m%d_%H')
                cycle_header_str = f"model={config.MODEL_NAME}, cycle={cycle_str}"
                print(f"[INFO] Wykryto cykl: {cycle_str}")

            # 2. Konwersja do DataFrame
            df = ds[VAR_SHORTNAME].to_dataframe().reset_index()

            # 3. Przetwarzanie czasu (valid_time -> yyyy, mm, dd, hh)
            # valid_time to czas, na który jest prognoza
            df['valid_time'] = pd.to_datetime(df['valid_time'])
            df['yyyy'] = df['valid_time'].dt.year
            df['mm'] = df['valid_time'].dt.month
            df['dd'] = df['valid_time'].dt.day
            df['hh'] = df['valid_time'].dt.hour

            # 4. Korekta długości geograficznej (-180 do 180)
            df['lon'] = df['longitude'].apply(lambda x: x if x <= 180 else x - 360)
            df['lat'] = df['latitude']
            
            # Zmiana nazwy zmiennej (jeśli inna niż w GRIB)
            df[VAR_CSV_NAME] = df[VAR_SHORTNAME]

            # 5. Wybór i kolejność kolumn
            target_cols = ['yyyy', 'mm', 'dd', 'hh', 'lat', 'lon', VAR_CSV_NAME]
            df = df[target_cols]

            # Opcjonalne zaokrąglanie dla zmniejszenia rozmiaru
            df['lat'] = df['lat'].round(3)
            df['lon'] = df['lon'].round(3)
            df[VAR_CSV_NAME] = df[VAR_CSV_NAME].round(1)

            all_dfs.append(df)
            print(f"[OK] Dodano dane z: {filename_in}")

        except Exception as e:
            print(f"[ERR] Błąd przy {filename_in}: {e}")

    # --- ZAPIS PLIKU ZBIORCZEGO ---
    if all_dfs:
        print("--- Łączenie danych i zapis ---")
        final_df = pd.concat(all_dfs, ignore_index=True)
        
        path_out = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

        with open(path_out, 'w', newline='') as f:
            # 1. Zapis linii nagłówkowej metadanych
            # Jeśli z jakiegoś powodu cycle_header_str jest pusty (nie wczytano plików), dajemy N/A
            header = cycle_header_str if cycle_header_str else f"model={config.MODEL_NAME}, cycle=UNKNOWN"
            f.write(header + "\n")
            
            # 2. Zapis danych CSV (bez indeksu)
            final_df.to_csv(f, index=False)

        print(f"✅ Sukces! Plik zapisany: {path_out}")
        print("Podgląd pierwszych linii:")
        # Wyświetlamy nagłówek pliku, żeby zweryfikować format
        with open(path_out, 'r') as f:
            for _ in range(5):
                print(f.readline().strip())
    else:
        print("❌ Nie przetworzono żadnych danych. Sprawdź pliki wejściowe.")

if __name__ == "__main__":
    main()