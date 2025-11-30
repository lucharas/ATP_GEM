import os
import warnings
import xarray as xr
import pandas as pd
import config 
import re

# Tłumienie ostrzeżeń
warnings.filterwarnings("ignore", category=FutureWarning)

# =======================
# KONFIGURACJA
# =======================
INPUT_DIR = config.DOWNLOAD_DIR
OUTPUT_DIR = os.path.join(config.RAW_DATA_DIR)
OUTPUT_FILENAME = "cdr_ea.csv"

# Wzór nazwy pliku GRIB do parsowania (dla CDR_entire_atmosphere_fXXX.grib)
FILENAME_PATTERN = re.compile(r"CDR_entire_atmosphere_f(?P<hour>\d{3})\.grib$")

# Mapowanie zmiennych
VAR_SHORTNAME = 'tcc'       # Nazwa w GRIB
VAR_CSV_NAME = 'tcc_ea'     # Nazwa w CSV (z sufiksem poziomu)

GRIB_FILTERS = {
    'shortName': VAR_SHORTNAME,
    'stepType': 'instant'
}

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    all_dfs = []  # Lista do trzymania ramek danych z każdej godziny
    cycle_header_str = None # Tutaj zapiszemy string z datą cyklu

    print(f"--- Start parsowania zbiorczego CDR Entire Atmosphere (Zmienna: {VAR_CSV_NAME}) ---")
    
    # Skanowanie katalogu w poszukiwaniu plików pasujących do wzoru
    for filename_in in sorted(os.listdir(INPUT_DIR)):
        match = FILENAME_PATTERN.match(filename_in)

        if match:
            fct_hour = int(match.group('hour'))
            
            path_in = os.path.join(INPUT_DIR, filename_in)

            try:
                # 1. Otwarcie Datasetu
                ds = xr.open_dataset(
                    path_in,
                    engine='cfgrib',
                    backend_kwargs={'filter_by_keys': GRIB_FILTERS}
                )

                # Pobranie daty cyklu (runu modelu) z pierwszego poprawnego pliku
                if cycle_header_str is None:
                    # Logika odczytu daty cyklu z pliku CDR_entire_atmosphere_fXXX.grib (ds.time)
                    if 'time' in ds.coords:
                        run_time = pd.to_datetime(ds.time.values)
                        cycle_str = run_time.strftime('%Y%m%d_%H')
                        cycle_header_str = f"model={config.MODEL_NAME}, cycle={cycle_str}"
                        print(f"[INFO] Wykryto cykl: {cycle_str} (źródło: time)")
                    else:
                         print(f"[ERR] Nie znaleziono GRIB refTime. Nagłówek będzie UNKNOWN.")


                # 2. Konwersja do DataFrame
                df = ds[VAR_SHORTNAME].to_dataframe().reset_index()

                # 3. Przetwarzanie czasu (valid_time -> yyyy, mm, dd, hh)
                df['valid_time'] = pd.to_datetime(df['valid_time'])
                df['yyyy'] = df['valid_time'].dt.year
                df['mm'] = df['valid_time'].dt.month
                df['dd'] = df['valid_time'].dt.day
                df['hh'] = df['valid_time'].dt.hour

                # 4. Korekta długości geograficznej (-180 do 180)
                df['lon'] = df['longitude'].apply(lambda x: x if x <= 180 else x - 360)
                df['lat'] = df['latitude']
                
                # Zmiana nazwy zmiennej na nową konwencję
                df = df.rename(columns={VAR_SHORTNAME: VAR_CSV_NAME})

                # 5. Wybór i kolejność kolumn
                target_cols = ['yyyy', 'mm', 'dd', 'hh', 'lat', 'lon', VAR_CSV_NAME]
                df = df[target_cols]

                # Opcjonalne zaokrąglanie
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
            header = cycle_header_str if cycle_header_str else f"model={config.MODEL_NAME}, cycle=UNKNOWN"
            f.write(header + "\n")
            
            final_df.to_csv(f, index=False)

        print(f"✅ Sukces! Plik zapisany: {path_out}")
        print("Podgląd pierwszych linii:")
        with open(path_out, 'r') as f:
            for _ in range(5):
                print(f.readline().strip())
    else:
        print("❌ Nie przetworzono żadnych danych. Sprawdź pliki wejściowe.")

if __name__ == "__main__":
    main()