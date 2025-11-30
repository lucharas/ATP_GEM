import os
import warnings
import xarray as xr
import pandas as pd
import config  # Import ustawień z pliku config.py

# Tłumienie ostrzeżeń (szczególnie cfgrib/pandas)
warnings.filterwarnings("ignore")

# =======================
# KONFIGURACJA SKRYPTU
# =======================
INPUT_DIR = config.DOWNLOAD_DIR
# Zapis bezpośrednio do raw_data
OUTPUT_DIR = config.RAW_DATA_DIR 
OUTPUT_FILENAME = "cdr_sfc.csv"

# Oczekiwane nazwy zmiennych w GRIB (shortName)
# Opady GFS w cfgrib są często mapowane jako 'tp' i 'cp' (Total/Convective Precipitation)
EXPECTED_GRIB_VARS = ['vis', 'cape', 'tp', 'cp'] 

# Mapowanie: Nazwa w GRIB -> Nazwa w CSV (zgodnie z życzeniem)
VAR_MAP = {
    'vis': 'vis',
    'cape': 'cape',
    'tp': 'apcp',  # Total Precipitation -> apcp
    'cp': 'acpcp'  # Convective Precipitation -> acpcp
}

# Filtr GRIB: Ładujemy wszystkie zmienne tylko dla poziomu 'surface'
GRIB_FILTERS = {
    'typeOfLevel': 'surface' 
    # Usunięto 'stepType', aby załadować Instant i Accum jednocześnie.
}

# Kolumny końcowe w pliku CSV
TARGET_COLS = ['yyyy', 'mm', 'dd', 'hh', 'lat', 'lon', 'vis', 'apcp', 'acpcp', 'cape']

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    all_dfs = [] 
    cycle_header_str = None 

    print(f"--- Start parsowania zbiorczego CDR SFC (Surface) w JEDNYM KROKU ---")
    
    for hour in config.FCT_HOURS_CDR:
        fct_str = f"{hour:03d}"
        filename_in = f"CDR_sfc_f{fct_str}.grib" 
        path_in = os.path.join(INPUT_DIR, filename_in)

        if not os.path.exists(path_in):
            print(f"[SKIP] Brak pliku: {filename_in}")
            continue

        try:
            # 1. Ładowanie wszystkich danych powierzchniowych w JEDNYM KROKU
            ds = xr.open_dataset(
                path_in,
                engine='cfgrib',
                backend_kwargs={'filter_by_keys': GRIB_FILTERS}
            )

            # Pobranie daty cyklu (runu modelu)
            if cycle_header_str is None:
                run_time = pd.to_datetime(ds.time.values)
                cycle_str = run_time.strftime('%Y%m%d_%H')
                cycle_header_str = f"model={config.MODEL_NAME}, cycle={cycle_str}"
                print(f"[INFO] Wykryto cykl: {cycle_str}")
            
            # 2. Konwersja do DataFrame
            grib_vars_to_load = [v for v in EXPECTED_GRIB_VARS if v in ds]
            
            if not grib_vars_to_load:
                print(f"[ERR] Nie znaleziono oczekiwanych zmiennych GRIB w pliku {filename_in}.")
                continue

            # Tworzymy DataFrame zawierający wszystkie zmienne
            df = ds[grib_vars_to_load].to_dataframe().reset_index()

            # 3. Przetwarzanie czasu (valid_time -> yyyy, mm, dd, hh)
            df['valid_time'] = pd.to_datetime(df['valid_time'])
            df['yyyy'] = df['valid_time'].dt.year
            df['mm'] = df['valid_time'].dt.month
            df['dd'] = df['valid_time'].dt.day
            df['hh'] = df['valid_time'].dt.hour
            
            # 4. Korekta Longitude i przypisanie lat/lon
            df['lon'] = df['longitude'].apply(lambda x: x if x <= 180 else x - 360)
            df['lat'] = df['latitude']
            
            # 5. Zmiana nazw kolumn na docelowe (np. tp -> apcp)
            df = df.rename(columns=VAR_MAP)

            # 6. Finalna struktura kolumn
            df_final = df[df.columns.intersection(TARGET_COLS)]
            # Uzupełnianie brakujących kolumn zerami (np. w f006 opady mogą być pominięte)
            for col in [c for c in TARGET_COLS if c not in df_final.columns]:
                df_final[col] = 0.0 

            df_final = df_final[TARGET_COLS]

            # 7. Zaokrąglanie wartości
            df_final['lat'] = df_final['lat'].round(3)
            df_final['lon'] = df_final['lon'].round(3)
            df_final['vis'] = df_final['vis'].round(0) 
            df_final['cape'] = df_final['cape'].round(0)
            df_final['apcp'] = df_final['apcp'].round(2)
            df_final['acpcp'] = df_final['acpcp'].round(2)

            all_dfs.append(df_final)
            print(f"[OK] Przetworzono: {filename_in}")

        except Exception as e:
            print(f"[CRITICAL] Błąd przy pliku {filename_in}: {e}")
            
    # --- ZAPIS PLIKU ZBIORCZEGO ---
    if all_dfs:
        print("--- Łączenie i zapis pliku zbiorczego ---")
        final_df = pd.concat(all_dfs, ignore_index=True)
        
        path_out = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

        with open(path_out, 'w', newline='') as f:
            header = cycle_header_str if cycle_header_str else f"model={config.MODEL_NAME}, cycle=UNKNOWN"
            f.write(header + "\n")
            final_df.to_csv(f, index=False)

        print(f"✅ Sukces! Plik zapisany: {path_out}")
        print("Podgląd pierwszych linii:")
        with open(path_out, 'r') as f:
            for _ in range(3):
                print(f.readline().strip())
    else:
        print("❌ Brak danych do zapisu.")

if __name__ == "__main__":
    main()