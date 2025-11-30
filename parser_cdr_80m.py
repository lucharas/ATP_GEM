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
OUTPUT_FILENAME = "cdr_80m.csv"

# Wzór nazwy pliku GRIB do parsowania (dla CDR_80m_fXXX.grib)
FILENAME_PATTERN = re.compile(r"CDR_80m_f(?P<hour>\d{3})\.grib$")

# Oczekiwane nazwy zmiennych w GRIB (t, q, u, v)
EXPECTED_GRIB_VARS = ['t', 'q', 'u', 'v'] 

# Mapowanie: Nazwa w GRIB -> Nazwa w CSV
VAR_MAP = {
    't': 'tmp_80m',   # Temperatura na 80m
    'q': 'sh_80m',    # Wilgotność właściwa na 80m
    'u': 'u_80m',     # Wiatr zonalny na 80m
    'v': 'v_80m',     # Wiatr merydionalny na 80m
}

# Filtr GRIB: Pusty
GRIB_FILTERS = {} 

# Kolumny końcowe w pliku CSV
TARGET_COLS = ['yyyy', 'mm', 'dd', 'hh', 'lat', 'lon', 'tmp_80m', 'sh_80m', 'u_80m', 'v_80m']

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    all_dfs = [] 
    cycle_header_str = None 

    print(f"--- Start parsowania zbiorczego CDR 80m (zmienne: {', '.join(EXPECTED_GRIB_VARS)}) ---")
    
    # Skanowanie katalogu w poszukiwaniu plików pasujących do wzoru
    for filename_in in sorted(os.listdir(INPUT_DIR)):
        match = FILENAME_PATTERN.match(filename_in)
        
        if match:
            fct_hour = int(match.group('hour'))
            
            path_in = os.path.join(INPUT_DIR, filename_in)

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

                # 3. Pobranie daty cyklu (tylko raz) - Używamy sprawdzonej, solidnej logiki
                if cycle_header_str is None:
                    run_time_found = False
                    source_var = 'UNKNOWN'

                    if 'forecast_reference_time' in ds.coords:
                        run_time = ds['forecast_reference_time'].values
                        run_time_found = True
                        source_var = 'forecast_reference_time'
                    elif 'time' in ds.coords:
                        run_time = ds['time'].values
                        run_time_found = True
                        source_var = 'time'
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
                
                # Uzupełnianie brakujących kolumn
                for col in [c for c in TARGET_COLS if c not in df_final.columns]:
                    df_final[col] = 0.0 

                df_final = df_final[TARGET_COLS]

                # 9. Zaokrąglanie wartości
                df_final['lat'] = df_final['lat'].round(3)
                df_final['lon'] = df_final['lon'].round(3)
                df_final['tmp_80m'] = df_final['tmp_80m'].round(2) 
                df_final['sh_80m'] = df_final['sh_80m'].round(5) 
                df_final['u_80m'] = df_final['u_80m'].round(3) 
                df_final['v_80m'] = df_final['v_80m'].round(3)

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