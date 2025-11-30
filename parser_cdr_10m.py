import os
import warnings
import xarray as xr
import pandas as pd
import config 
import re 

# Tłumienie ostrzeżeń
warnings.filterwarnings("ignore")

# =======================
# KONFIGURACJA SKRYPTU
# =======================
INPUT_DIR = config.DOWNLOAD_DIR
OUTPUT_DIR = config.RAW_DATA_DIR 
OUTPUT_FILENAME = "cdr_10m.csv"

# Wzór nazwy pliku GRIB do parsowania (dla CDR_10m_fXXX.grib)
FILENAME_PATTERN = re.compile(r"CDR_10m_f(?P<hour>\d{3})\.grib$")

# Oczekiwane nazwy zmiennych w GRIB
EXPECTED_GRIB_VARS = ['u10', 'v10'] 

# Mapowanie: Nazwa w GRIB -> Nazwa w CSV
VAR_MAP = {
    'u10': 'u_10m',   # Wiatr zonalny na 10m
    'v10': 'v_10m',   # Wiatr merydionalny na 10m
}

# Kolumny końcowe w pliku CSV
TARGET_COLS = ['yyyy', 'mm', 'dd', 'hh', 'lat', 'lon', 'u_10m', 'v_10m']

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    all_dfs = [] 
    cycle_header_str = None 
    
    print(f"--- Start parsowania zbiorczego CDR 10m (Zmienne: {', '.join(EXPECTED_GRIB_VARS)}) ---")
    
    # KOREKTA LOGIKI: Iteracja przez KAŻDY plik pasujący do wzoru
    for filename_in in sorted(os.listdir(INPUT_DIR)):
        match = FILENAME_PATTERN.match(filename_in)
        
        if match:
            fct_hour = int(match.group('hour'))
            
            path_in = os.path.join(INPUT_DIR, filename_in)

            try:
                # 1. Ładowanie danych z pojedynczego kroku prognozy
                ds = xr.open_dataset(
                    path_in,
                    engine='cfgrib',
                )
                
                # 2. Definicja listy zmiennych do załadowania
                grib_vars_to_load = [v for v in EXPECTED_GRIB_VARS if v in ds]
                
                if not grib_vars_to_load:
                    print(f"[ERR] Nie znaleziono oczekiwanych zmiennych GRIB w pliku {filename_in}.")
                    continue

                # 3. Pobranie daty cyklu (tylko raz, z pierwszego pliku)
                if cycle_header_str is None:
                    run_time_found = False
                    
                    if 'forecast_reference_time' in ds.coords:
                        run_time = ds['forecast_reference_time'].values
                        run_time_found = True
                    elif 'time' in ds.coords:
                        run_time = ds['time'].values
                        run_time_found = True
                    else:
                        for var_name in grib_vars_to_load: 
                            if 'GRIB_refTime' in ds[var_name].attrs: 
                                run_time = ds[var_name].attrs['GRIB_refTime']
                                run_time_found = True
                                break
                                
                    if run_time_found:
                        run_time = pd.to_datetime(run_time)
                        cycle_str = run_time.strftime('%Y%m%d_%H')
                        cycle_header_str = f"model={config.MODEL_NAME}, cycle={cycle_str}"
                        print(f"[INFO] Wykryto cykl: {cycle_str}")
                    else:
                        cycle_header_str = f"model={config.MODEL_NAME}, cycle=UNKNOWN"
                        print(f"[ERR] Nie znaleziono GRIB refTime. Nagłówek będzie UNKNOWN.")


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
                df_final = df_final[TARGET_COLS] # Zapewnienie kolejności

                # 9. Zaokrąglanie wartości (wiatr - 4 miejsca po przecinku)
                df_final['lat'] = df_final['lat'].round(3)
                df_final['lon'] = df_final['lon'].round(3)
                df_final['u_10m'] = df_final['u_10m'].round(4) 
                df_final['v_10m'] = df_final['v_10m'].round(4) 

                all_dfs.append(df_final)
                print(f"[OK] Przetworzono i dodano dane z: {filename_in}")

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
        print(f"Ilość przetworzonych kroków czasowych: {len(final_df['hh'].unique())}")
    else:
        print("❌ Brak danych do zapisu. Sprawdź pliki.")

if __name__ == "__main__":
    main()