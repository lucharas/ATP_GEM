import os
import warnings
import xarray as xr
import pandas as pd
import config 

# Tłumienie ostrzeżeń
warnings.filterwarnings("ignore")

# =======================
# KONFIGURACJA SKRYPTU
# =======================
INPUT_DIR = config.DOWNLOAD_DIR
OUTPUT_DIR = config.RAW_DATA_DIR 
OUTPUT_FILENAME = "cdr_2m.csv"

# Oczekiwane nazwy zmiennych w GRIB (shortName) ZGODNE Z DIAGNOSTYKĄ
EXPECTED_GRIB_VARS = ['t2m', 'sh2', 'r2'] 

# Mapowanie: Nazwa w GRIB -> Nazwa w CSV
VAR_MAP = {
    't2m': 'tmp',   # Temperatura 2m
    'sh2': 'spfh',  # Wilgotność właściwa 2m
    'r2': 'rh2',    # Wilgotność względna 2m
}

# Filtr GRIB: Pusty, ponieważ plik nie koduje poziomu w metadanych
GRIB_FILTERS = {} 

# Kolumny końcowe w pliku CSV
TARGET_COLS = ['yyyy', 'mm', 'dd', 'hh', 'lat', 'lon', 'tmp', 'spfh', 'rh2']

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    all_dfs = [] 
    cycle_header_str = None 

    print(f"--- Start parsowania zbiorczego CDR 2m (zmienne: {', '.join(EXPECTED_GRIB_VARS)}) ---")
    
    for hour in config.FCT_HOURS_CDR:
        fct_str = f"{hour:03d}"
        filename_in = f"CDR_2m_f{fct_str}.grib"
        path_in = os.path.join(INPUT_DIR, filename_in)

        if not os.path.exists(path_in):
            print(f"[SKIP] Brak pliku: {filename_in}")
            continue

        try:
            # 1. Ładowanie wszystkich danych (bez filtrów poziomu)
            ds = xr.open_dataset(
                path_in,
                engine='cfgrib',
            )
            
            # 2. Definicja listy zmiennych do załadowania
            grib_vars_to_load = [v for v in EXPECTED_GRIB_VARS if v in ds]
            
            if not grib_vars_to_load:
                print(f"[ERR] Nie znaleziono oczekiwanych zmiennych GRIB w pliku {filename_in}.")
                continue

            # 3. Pobranie daty cyklu (tylko raz)
            if cycle_header_str is None:
                run_time_found = False
                source_var = 'UNKNOWN'

                # PRÓBA 1: Sprawdzenie najbardziej prawdopodobnej koordynaty referencyjnej
                if 'forecast_reference_time' in ds.coords:
                    run_time = ds['forecast_reference_time'].values
                    run_time_found = True
                    source_var = 'forecast_reference_time'
                # PRÓBA 2: Koordynata 'time' (używana w innych plikach)
                elif 'time' in ds.coords:
                    run_time = ds['time'].values
                    run_time_found = True
                    source_var = 'time'
                # PRÓBA 3: Odczyt GRIB_refTime z atrybutów zmiennej (backup)
                else:
                    for var_name in grib_vars_to_load: 
                        if 'GRIB_refTime' in ds[var_name].attrs: 
                            run_time = ds[var_name].attrs['GRIB_refTime']
                            run_time_found = True
                            source_var = var_name + ' GRIB_refTime'
                            break
                            
                if run_time_found:
                    run_time = pd.to_datetime(run_time)
                    cycle_str = run_time.strftime('%Y%m%d_%H')
                    cycle_header_str = f"model={config.MODEL_NAME}, cycle={cycle_str}"
                    print(f"[INFO] Wykryto cykl: {cycle_str} (źródło: {source_var})")
                else:
                    print(f"[ERR] Nie znaleziono GRIB refTime w żadnym typowym miejscu. Nagłówek będzie UNKNOWN.")


            # 4. Konwersja do DataFrame
            df = ds[grib_vars_to_load].to_dataframe().reset_index()

            # 5. Przetwarzanie czasu (valid_time -> yyyy, mm, dd, hh)
            df['valid_time'] = pd.to_datetime(df['valid_time'])
            df['yyyy'] = df['valid_time'].dt.year
            df['mm'] = df['valid_time'].dt.month
            df['dd'] = df['valid_time'].dt.day
            df['hh'] = df['valid_time'].dt.hour
            
            # 6. Korekta Longitude i przypisanie lat/lon
            df['lon'] = df['longitude'].apply(lambda x: x if x <= 180 else x - 360)
            df['lat'] = df['latitude']
            
            # 7. Zmiana nazw kolumn na docelowe
            df = df.rename(columns=VAR_MAP)

            # 8. Finalna struktura kolumn
            df_final = df[df.columns.intersection(TARGET_COLS)]
            
            # Uzupełnianie brakujących kolumn
            for col in [c for c in TARGET_COLS if c not in df_final.columns]:
                df_final[col] = 0.0 

            df_final = df_final[TARGET_COLS]

            # 9. Zaokrąglanie wartości
            df_final['lat'] = df_final['lat'].round(3)
            df_final['lon'] = df_final['lon'].round(3)
            df_final['tmp'] = df_final['tmp'].round(2) 
            df_final['spfh'] = df_final['spfh'].round(5) 
            df_final['rh2'] = df_final['rh2'].round(1)

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