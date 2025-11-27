import tkinter as tk
from tkinter import ttk
import datetime
from PIL import Image, ImageTk
import os
import threading 

# --- Importujemy moduły logiczne
import config
import data_downloader

# Zmieniamy wersję na 1.0_thread, aby widzieć, że obsługa wątków działa
versja = "1.0_thread" 

# --- KONFIGURACJA STYLÓW ---
MONOSPACE_FONT = ("Courier", 10)
BOLD_TAG = "bold_dynamic"
IMAGE_FILE = "ico_ATP.png" 

# --- FUNKCJE LOGICZNE ---

def generate_header_content(charakter: str, nazwa_komunikatu: str, start_modelu_dt: datetime.datetime, start_prognozy_dt: datetime.datetime) -> list:
    """Generuje listę wierszy nagłówka i ich status pogrubienia (True dla dynamicznych/czarnych)."""
    
    dtg_time = datetime.datetime.utcnow()
    dtg_str = dtg_time.strftime("%d%H%MZ%b%Y").upper()

    start_modelu_str = start_modelu_dt.strftime("%d%H%MZ%b%Y").upper()
    start_prognozy_str = start_prognozy_dt.strftime("%d%H%MZ%b%Y").upper()
    
    # Koniec prognozy to Start Prognozy + Horyzont BWR (48h)
    koniec_prognozy_dt = start_prognozy_dt + datetime.timedelta(hours=config.HORIZON_HR_BWR) 
    koniec_prognozy_str = koniec_prognozy_dt.strftime("%d%H%MZ%b%Y").upper()
    
    header_data = [
        (f"{charakter}/{nazwa_komunikatu}/-//", True), 
        (config.ATP_MSGID_LINE, False), 
        (config.ATP_GEODATUM_LINE, False), 
        (f"DTG/{dtg_str}//", True), 
        (config.ATP_AREAM_LINE, False), 
        (f"ZULUM/{start_modelu_str}/{start_prognozy_str}/{koniec_prognozy_str}//", True), 
        (config.ATP_UNITM_LINE, False), 
    ]
    
    return header_data

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(f"Generator Meldunków ATP (GFS 0.25) v{versja}")
        self.geometry("800x650")

        today = datetime.datetime.utcnow().date()
        tomorrow = today + datetime.timedelta(days=1)
        # Inicjalizacja wartości z config.py
        self.start_modelu_dt = datetime.datetime.combine(today, datetime.time(config.CYCLE_HOUR, 0))
        self.start_prognozy_dt = datetime.datetime.combine(tomorrow, datetime.time(config.PRODUCT_START_HOUR, 0))
        
        self.photo = None 
        self.execute_button = None # Referencja do przycisku 'Wykonaj'

        self.create_widgets()
        self.update_header()

    def create_widgets(self):
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- SEKCJA 1: PARAMETRY WEJŚCIOWE ---
        param_frame = ttk.LabelFrame(main_frame, text="Parametry wejściowe", padding="10")
        param_frame.pack(fill=tk.X, pady=10)
        
        param_frame.columnconfigure(1, weight=0)
        param_frame.columnconfigure(4, weight=1) 
        
        # 1. Charakter Meldunku
        ttk.Label(param_frame, text="Charakter meldunku:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.charakter_var = tk.StringVar(value="EXER")
        charakter_options = ["OPER", "EXER"]
        ttk.Combobox(param_frame, textvariable=self.charakter_var, values=charakter_options, state="readonly").grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        # 2. Nazwa Komunikatu
        ttk.Label(param_frame, text="Nazwa komunikatu:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.nazwa_komunikatu_var = tk.StringVar(value="TESTCOAS")
        ttk.Entry(param_frame, textvariable=self.nazwa_komunikatu_var).grid(row=1, column=1, padx=5, pady=5, sticky="ew")

        # 3. Czas Startu Modelu (Data + Godzina)
        ttk.Label(param_frame, text="Start modelu (UTC):").grid(row=2, column=0, padx=5, pady=5, sticky="w")
        self.model_data_var = tk.StringVar(value=self.start_modelu_dt.strftime("%Y-%m-%d"))
        self.model_hh_var = tk.StringVar(value=self.start_modelu_dt.strftime("%H"))
        ttk.Entry(param_frame, textvariable=self.model_data_var).grid(row=2, column=1, padx=5, pady=5, sticky="w")
        ttk.Combobox(param_frame, textvariable=self.model_hh_var, values=["00", "06", "12", "18"], width=5, state="readonly").grid(row=2, column=2, padx=5, pady=5, sticky="w")
        ttk.Label(param_frame, text="HH").grid(row=2, column=3, sticky="w")

        # 4. Czas Startu Prognozy (Data + Godzina)
        ttk.Label(param_frame, text="Start prognozy (UTC):").grid(row=3, column=0, padx=5, pady=5, sticky="w")
        self.prognoza_data_var = tk.StringVar(value=self.start_prognozy_dt.strftime("%Y-%m-%d"))
        self.prognoza_hh_var = tk.StringVar(value=self.start_prognozy_dt.strftime("%H"))
        ttk.Entry(param_frame, textvariable=self.prognoza_data_var).grid(row=3, column=1, padx=5, pady=5, sticky="w")
        ttk.Combobox(param_frame, textvariable=self.prognoza_hh_var, values=["00", "06", "12", "18"], width=5, state="readonly").grid(row=3, column=2, padx=5, pady=5, sticky="w")
        ttk.Label(param_frame, text="HH").grid(row=3, column=3, sticky="w")
        
        # 5. Pola Meldunku
        ttk.Label(param_frame, text="").grid(row=4, column=0, padx=5, pady=5, sticky="w")
        self.cdr_var = tk.BooleanVar(value=True)
        self.bwr_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(param_frame, text="CDR", variable=self.cdr_var).grid(row=4, column=1, padx=5, pady=5, sticky="w")
        ttk.Checkbutton(param_frame, text="BWR", variable=self.bwr_var).grid(row=4, column=2, padx=5, pady=5, sticky="w")


        # --- LOGO ATP (ICO_ATP.JPG) ---
        try:
            # Używamy ścieżki do pliku, który został uploadowany przez użytkownika
            img_path = IMAGE_FILE if os.path.exists(IMAGE_FILE) else 'ico_ATP.jpg' 
            img = Image.open(img_path) 
            img = img.resize((120, 120), Image.Resampling.LANCZOS)
            self.photo = ImageTk.PhotoImage(img)
            logo_label = ttk.Label(param_frame, image=self.photo)
            logo_label.grid(row=0, column=4, rowspan=999, padx=10, pady=5, sticky="e") 
        except FileNotFoundError:
            self.log_message(f"[WARN] Nie znaleziono pliku {IMAGE_FILE}.")
        except Exception as e:
            self.log_message(f"[ERROR] Błąd ładowania logo: {e}")


        # --- SEKCJA 2: PRZYCISKI AKCJI ---
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)

        ttk.Button(button_frame, text="Odśwież nagłówek", command=self.update_header).pack(side=tk.LEFT, padx=10)
        
        # Zapisujemy referencję do przycisku "Wykonaj"
        self.execute_button = ttk.Button(button_frame, text="Wykonaj (Pobieranie/Parsowanie)", command=self.execute_task)
        self.execute_button.pack(side=tk.LEFT, padx=10)


        # --- SEKCJA 3: PODGLĄD NAGŁÓWKA ---
        header_frame = ttk.LabelFrame(main_frame, text="Wspólna część nagłówkowa", padding="10")
        header_frame.pack(fill=tk.X, pady=10)
        self.header_text = tk.Text(header_frame, height=7, font=MONOSPACE_FONT, relief=tk.RIDGE)
        self.header_text.pack(fill=tk.X)
        self.header_text.tag_config(BOLD_TAG, font=(MONOSPACE_FONT[0], MONOSPACE_FONT[1], "bold"))
        self.header_text.bind("<MouseWheel>", lambda e: "break")
        

        # --- SEKCJA 4: LOGI I KOMUNIKATY ---
        log_frame = ttk.LabelFrame(main_frame, text="Komunikaty & logi", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True)
        
        self.log_text = tk.Text(log_frame, height=10, font=MONOSPACE_FONT, state=tk.DISABLED, relief=tk.SUNKEN)
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def log_message(self, message: str):
        """Dodaje komunikat do logów i zapewnia przewijanie."""
        self.log_text.config(state=tk.NORMAL)
        current_time = datetime.datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"{current_time} {message}\n")
        
        # Ograniczenie liczby linii logów do 20, aby nie przeładować pamięci
        #if int(self.log_text.index('end-1c').split('.')[0]) > 20:
        #    self.log_text.delete("1.0", "2.0")
            
         # Ograniczenie liczby linii logów; 0 w config.LOG_MAX_LINES oznacza brak limitu
        max_lines = getattr(config, "LOG_MAX_LINES", 0)
        if max_lines > 0:
            while int(self.log_text.index('end-1c').split('.')[0]) > max_lines:
                self.log_text.delete("1.0", "2.0")

        self.log_text.see(tk.END)
        
        # Używamy .update_idletasks(), aby wymusić odświeżenie GUI PO wstawieniu logu
        # Jest to bezpieczna metoda, gdy wywołujemy log_message z wątku w tle
        self.update_idletasks() 
        
        self.log_text.config(state=tk.DISABLED)

    def update_header(self):
        """Aktualizuje zawartość sekcji nagłówka."""
        try:
            model_date = datetime.datetime.strptime(self.model_data_var.get(), "%Y-%m-%d").date()
            prognoza_date = datetime.datetime.strptime(self.prognoza_data_var.get(), "%Y-%m-%d").date()
            
            self.start_modelu_dt = datetime.datetime.combine(model_date, datetime.time(int(self.model_hh_var.get()), 0))
            self.start_prognozy_dt = datetime.datetime.combine(prognoza_date, datetime.time(int(self.prognoza_hh_var.get()), 0))
            
            self.log_message(f"[INFO] Czas Modelu: {self.start_modelu_dt.strftime('%Y-%m-%d %H:%M')}Z.")

        except ValueError:
            self.log_message("[ERROR] Nieprawidłowy format daty/godziny! Użyj RRRR-MM-DD i HH.")
            return

        header_data = generate_header_content(
            self.charakter_var.get().upper(),
            self.nazwa_komunikatu_var.get().upper(),
            self.start_modelu_dt,
            self.start_prognozy_dt
        )

        self.header_text.config(state=tk.NORMAL)
        self.header_text.delete("1.0", tk.END)
        
        for content, is_dynamic in header_data:
            self.header_text.insert(tk.END, content + "\n")
            
            if is_dynamic:
                start_index = self.header_text.index("end-2l")
                end_index = self.header_text.index("end-1l")
                self.header_text.tag_add(BOLD_TAG, start_index, end_index)

        self.header_text.config(state=tk.DISABLED)
        self.log_message("[INFO] Nagłówek został odświeżony.")
        
    def _download_and_process_task(self):
        """Metoda wykonująca obciążające zadanie w osobnym wątku."""
        
        is_cdr = self.cdr_var.get()
        is_bwr = self.bwr_var.get()
            
        self.log_message(f"[PROGRESS] Generowanie URL dla Model Run: {self.start_modelu_dt.strftime('%Y-%m-%d %H')}Z...")
        
        try:
            # 1. Generowanie URL
            grouped_urls = data_downloader.generate_gfs_urls(
                self.start_modelu_dt, 
                is_cdr=is_cdr, 
                is_bwr=is_bwr
            )
            total_urls = sum(len(urls) for urls in grouped_urls.values())
            self.log_message(f"[INFO] Wygenerowano łącznie {total_urls} plików GRIB do pobrania.")
            
            if total_urls == 0:
                self.log_message("[WARN] Brak plików do pobrania. Przerwano.")
                return

            # 2. Pobieranie plików z raportowaniem postępu
            self.log_message(f"[PROGRESS] Rozpoczęcie pobierania danych GFS do katalogu: {config.DOWNLOAD_DIR}...")
            
            # Pobieranie plików z raportowaniem postępu do self.log_message
            success = data_downloader.download_grib_files(grouped_urls, self.log_message, config.DOWNLOAD_DIR)

            # 3. Parsowanie i Generowanie Meldunków (Symulacja na razie)
            if success:
                self.log_message("[SUCCESS] Pobieranie zakończone pomyślnie.")
                self.log_message("[PROGRESS] Rozpoczęcie parsowania plików GRIB i generowania meldunków...")
                # Tutaj będzie właściwa logika parsowania i generowania meldunków
                self.log_message("[SUCCESS] Zakończono generowanie meldunków ATP.")
            else:
                self.log_message("[FATAL] Pobieranie zakończone BŁĘDEM. Meldunki nie zostaną wygenerowane.")

        except Exception as e:
            self.log_message(f"[CRITICAL] Wystąpił nieoczekiwany błąd: {type(e).__name__}: {e}")
        
        self.log_message("-" * 25)
        
        # Uaktywnienie przycisku po zakończeniu
        # Sprawdzamy, czy referencja istnieje, aby uniknąć błędów
        if self.execute_button:
            self.execute_button.config(state=tk.NORMAL) 

    def execute_task(self):
        """Metoda startująca wątek pobierania."""
        self.update_header()
        self.log_message("-" * 25)
        self.log_message("[TASK] Rozpoczęcie zadania Wykonaj...")
        
        if not self.cdr_var.get() and not self.bwr_var.get():
            self.log_message("[WARN] Nie wybrano żadnego pola meldunku (CDR/BWR). Przerwano.")
            return

        # Zabezpieczenie przed wielokrotnym kliknięciem
        if self.execute_button:
            self.execute_button.config(state=tk.DISABLED)
            self.log_message("[INFO] Blokowanie przycisku 'Wykonaj' - trwa pobieranie...")
        
        # Startowanie wątku pobierania w tle
        download_thread = threading.Thread(target=self._download_and_process_task)
        download_thread.start()


if __name__ == "__main__":
    app = App()
    app.mainloop()