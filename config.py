import datetime

# =======================
# 1. NOMADS i Ustawienia Ogólne
# =======================
BASE_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25_1hr.pl"
DOWNLOAD_TIMEOUT = 60 # Sekundy na timeout zapytania HTTP
MODEL_NAME = "GFS_0p25_1hr"
DOWNLOAD_DIR = "grib_data" # Katalog, do którego będą pobierane pliki GRIB
LOG_MAX_LINES = 200

# =======================
# 2. CYKL MODELU I HORYZONTY CZASOWE
# =======================
CYCLE_HOUR = 0               # Godzina startu cyklu modelu (np. 00Z)
PRODUCT_START_HOUR = 6       # Godzina startu prognozy w meldunku (np. F006)

# --- Ustawienia CDR ---
CADENCE_HR_CDR = 2       # Interwał co 2h
#HORIZON_HR_CDR = 24      # Czas trwania meldunku po starcie prognozy (24h)
HORIZON_HR_CDR = 6      # Czas trwania meldunku po starcie prognozy (24h)

# --- Ustawienia BWR ---
CADENCE_HR_BWR = 6       # Interwał co 6h
#HORIZON_HR_BWR = 48      # Czas trwania meldunku po starcie prognozy (48h)
HORIZON_HR_BWR = 12      # Czas trwania meldunku po starcie prognozy (48h)


# =======================
# 3. WYGENEROWANE LISTY GODZIN FCT
# =======================

# CDR: F006, F008, ..., F030. (Start od godziny PRODUCT_START_HOUR, co 2h)
FCT_HOURS_CDR = list(range(
    PRODUCT_START_HOUR,
    PRODUCT_START_HOUR + HORIZON_HR_CDR + CADENCE_HR_CDR,
    CADENCE_HR_CDR
))
# BWR: F012, F018, ..., F054. (Start od PRODUCT_START_HOUR + 6h, co 6h)
FCT_HOURS_BWR = list(range(
    PRODUCT_START_HOUR + CADENCE_HR_BWR,
    PRODUCT_START_HOUR + HORIZON_HR_BWR + CADENCE_HR_BWR,
    CADENCE_HR_BWR
))


# =======================
# 4. OBSZARY BBOX (Współrzędne do filtra Nomads)
# =======================
BBOX_CDR = dict(leftlon=13.00, rightlon=25.00, bottomlat=49.00, toplat=60.00)
BBOX_BWR = dict(leftlon=13.00, rightlon=40.00, bottomlat=43.00, toplat=60.00)


# =======================
# 5. POLA DANYCH GFS
# =======================

# --- CDR: Pola na stałych poziomach wysokościowych ---
FIELDS_CDR = [
    # Wiatr (komponenty u/v)
    ("UGRD", "lev_10_m_above_ground", "u10"),
    ("VGRD", "lev_10_m_above_ground", "v10"),
    ("UGRD", "lev_80_m_above_ground", "u80"),
    ("VGRD", "lev_80_m_above_ground", "v80"),

    # Temperatura
    ("TMP", "lev_2_m_above_ground", "t2"),
    ("TMP", "lev_80_m_above_ground", "t80"),

    # Wilgotność
    ("SPFH", "lev_2_m_above_ground", "q2"),
    ("SPFH", "lev_80_m_above_ground", "q80"),
    ("RH", "lev_2_m_above_ground", "rh2"),

    # Widzialność, chmury, opad, chwiejność (Surface / Entire Atmosphere)
    ("VIS", "lev_surface", "vis"),
    ("TCDC", "lev_entire_atmosphere", "tcc"),
    ("APCP", "lev_surface", "apcp"),
    ("ACPCP", "lev_surface", "acpcp"),
    ("CAPE", "lev_surface", "cape"),
    ("CIN", "lev_surface", "cin"),
    ("4LFTX", "lev_surface", "lftx"),
]

# --- BWR: Pola na poziomach barycznych ---
# Uwaga: BWR pobiera wszystkie poziomy w jednym żądaniu na godzinę prognozy
# (patrz data_downloader.generate_gfs_urls), co upraszcza dalsze parsowanie.
BWR_VARS = ("UGRD", "VGRD", "TMP", "RH", "HGT")
BWR_LEVELS_MB = [900, 700, 500, 400, 300, 250, 150, 100, 70, 50, 30, 20, 10]

# Pełne nazwy poziomów dla URL
BWR_LEVELS_MB_URL = [f"lev_{mb}_mb" for mb in BWR_LEVELS_MB]


# =======================
# 6. STRUKTURY DANYCH DO POBIERANIA URL (GENEROWANE AUTOMATYCZNIE)
# =======================

# Grupowanie pól CDR w bloki wg poziomu GFS (dla jednego pliku GRIB)
CDR_BLOCKS = {
    "sfc": ["lev_surface"],
    "2m": ["lev_2_m_above_ground"],
    "10m": ["lev_10_m_above_ground"],
    "80m": ["lev_80_m_above_ground"],
    "entire_atmosphere": ["lev_entire_atmosphere"]
}

# Mapa pól GFS: LEV (poziom) : [VAR1, VAR2, ...]
CDR_URL_MAP = {}
for var, lev, name in FIELDS_CDR:
    if lev not in CDR_URL_MAP:
        CDR_URL_MAP[lev] = []
    if var not in CDR_URL_MAP[lev]: # Unikamy duplikatów
        CDR_URL_MAP[lev].append(var)


# =======================
# 7. NAGŁÓWKI MELDUNKÓW (Statyczne linie)
# =======================
ATP_FIRST_LINE = "EXER/TESTCOAS/-//"
ATP_MSGID_LINE = "MSGID/CBRN CDR/IMGW-PIB/-/-/-/-/-/-/-/-//"
ATP_GEODATUM_LINE = "GEODATUM/WGE//"
ATP_AREAM_LINE = "AREAM/NEEB111//"
ATP_UNITM_LINE = "UNITM/-/DGG/KPH/C//"


# =======================
# 8. OBSZARY BWR i CDR (DANE GEOGRAFICZNE)
# =======================

BWR_AREAS= [
    ("NFEB31", 53.750, 13.750, 52.5, 55.0, 12.5, 15.0),
    ("NFEB24", 53.750, 16.250, 52.5, 55.0, 15.0, 17.5),
    ("NFEB21", 53.750, 18.750, 52.5, 55.0, 17.5, 20.0),
    ("NFEC34", 53.750, 21.250, 52.5, 55.0, 20.0, 22.5),
    ("NFEC31", 53.750, 23.750, 52.5, 55.0, 22.5, 25.0),
    ("NFEB32", 51.250, 13.750, 50.0, 52.5, 12.5, 15.0),
    ("NFEB23", 51.250, 16.250, 50.0, 52.5, 15.0, 17.5),
    ("NFEB22", 51.250, 18.750, 50.0, 52.5, 17.5, 20.0),
    ("NFEC33", 51.250, 21.250, 50.0, 52.5, 20.0, 22.5),
    ("NFEC32", 51.250, 23.750, 50.0, 52.5, 22.5, 25.0),
    ("NEEB11", 48.750, 18.750, 47.5, 50.0, 17.5, 20.0),
    ("NEEC44", 48.750, 21.250, 47.5, 50.0, 20.0, 22.5),
    ("NEEC41", 48.750, 23.750, 47.5, 50.0, 22.5, 25.0),
    ("NFEB11", 58.750, 18.750, 57.5, 60.0, 17.5, 20.0),
    ("NFEB12", 56.250, 18.750, 55.0, 57.5, 17.5, 20.0),
    ("NFEB13", 56.250, 16.250, 55.0, 57.5, 15.0, 17.5),
    ("NFEB14", 58.750, 16.250, 57.5, 60.0, 15.0, 17.5),
    ("NFEB41", 58.750, 13.750, 57.5, 60.0, 12.5, 15.0),
    ("NFEB42", 56.250, 13.750, 55.0, 57.5, 12.5, 15.0),
    ("NFEC12", 56.250, 28.750, 55.0, 57.5, 27.5, 30.0),
    ("NFEC13", 56.250, 26.250, 55.0, 57.5, 25.0, 27.5),
    ("NFEC21", 53.750, 28.750, 52.5, 55.0, 27.5, 30.0),
    ("NFEC22", 51.250, 28.750, 50.0, 52.5, 27.5, 30.0),
    ("NFEC23", 51.250, 26.250, 50.0, 52.5, 25.0, 27.5),
    ("NFEC24", 53.750, 26.250, 52.5, 55.0, 25.0, 27.5),
    ("NFED31", 53.750, 33.750, 52.5, 55.0, 32.5, 35.0),
    ("NFED32", 51.250, 33.750, 50.0, 52.5, 32.5, 35.0),
    ("NFED33", 51.250, 31.250, 50.0, 52.5, 30.0, 32.5),
    ("NFED34", 53.750, 31.250, 52.5, 55.0, 30.0, 32.5),
    ("NEEC11", 48.750, 28.750, 47.5, 50.0, 27.5, 30.0),
    ("NEEC12", 46.250, 28.750, 45.0, 47.5, 27.5, 30.0),
    ("NEEC14", 48.750, 26.250, 47.5, 50.0, 25.0, 27.5),
    ("NEED41", 48.750, 33.750, 47.5, 50.0, 32.5, 35.0),
    ("NEED42", 46.250, 33.750, 45.0, 47.5, 32.5, 35.0),
    ("NEED43", 46.250, 31.250, 45.0, 47.5, 30.0, 32.5),
    ("NEED44", 48.750, 31.250, 47.5, 50.0, 30.0, 32.5),
    ("NEED11", 48.750, 38.750, 47.5, 50.0, 37.5, 40.0),
    ("NEED13", 46.250, 36.250, 45.0, 47.5, 35.0, 37.5),
    ("NEED14", 48.750, 36.250, 47.5, 50.0, 35.0, 37.5),
    ("NEED31", 43.750, 33.750, 42.5, 45.0, 32.5, 35.0),
    ("NFED22", 51.250, 38.750, 50.0, 52.5, 37.5, 40.0),
    ("NFED23", 51.250, 36.250, 50.0, 52.5, 35.0, 37.5),
    ("NFED24", 53.750, 36.250, 52.5, 55.0, 35.0, 37.5),
    ("NFEC42", 56.250, 23.750, 55.0, 57.5, 22.5, 25.0),
    ("NFEC43", 56.250, 21.250, 55.0, 57.5, 20.0, 22.5),
]

CDR_AREAS = [
    ("NFEB322", 50.625, 14.375, 50.0, 51.25, 13.75, 15.0),
    ("NFEB232", 50.625, 16.875, 50.0, 51.25, 16.25, 17.5),
    ("NFEB233", 50.625, 15.625, 50.0, 51.25, 15.0, 16.25),
    ("NFEB212", 53.125, 19.375, 52.5, 53.75, 18.75, 20.0),
    ("NFEB213", 53.125, 18.125, 52.5, 53.75, 17.5, 18.75),
    ("NFEC324", 51.875, 23.125, 51.25, 52.5, 22.5, 23.75),
    ("NFEC323", 50.625, 23.125, 50.0, 51.25, 22.5, 23.75),
    ("NFEC322", 50.625, 24.375, 50.0, 51.25, 23.75, 25.0),
    ("NFEB321", 51.875, 14.375, 51.25, 52.5, 13.75, 15.0),
    ("NFEB234", 51.875, 15.625, 51.25, 52.5, 15.0, 16.25),
    ("NFEB221", 51.875, 19.375, 51.25, 52.5, 18.75, 20.0),
    ("NEEC444", 49.375, 20.625, 48.75, 50.0, 20.0, 21.25),
    ("NFEC343", 53.125, 20.625, 52.5, 53.75, 20.0, 21.25),
    ("NFEC334", 51.875, 20.625, 51.25, 52.5, 20.0, 21.25),
    ("NFEC331", 51.875, 21.875, 51.25, 52.5, 21.25, 22.5),
    ("NFEB223", 50.625, 18.125, 50.0, 51.25, 17.5, 18.75),
    ("NEEC441", 49.375, 21.875, 48.75, 50.0, 21.25, 22.5),
    ("NEEC414", 49.375, 23.125, 48.75, 50.0, 22.5, 23.75),
    ("NFEC314", 54.375, 23.125, 53.75, 55.0, 22.5, 23.75),
    ("NFEC313", 53.125, 23.125, 52.5, 53.75, 22.5, 23.75),
    ("NFEC312", 53.125, 24.375, 52.5, 53.75, 23.75, 25.0),
    ("NFEC342", 53.125, 21.875, 52.5, 53.75, 21.25, 22.5),
    ("NFEB241", 54.375, 16.875, 53.75, 55.0, 16.25, 17.5),
    ("NFEB214", 54.375, 18.125, 53.75, 55.0, 17.5, 18.75),
    ("NFEB211", 54.375, 19.375, 53.75, 55.0, 18.75, 20.0),
    ("NEEB114", 49.375, 18.125, 48.75, 50.0, 17.5, 18.75),
    ("NEEB111", 49.375, 19.375, 48.75, 50.0, 18.75, 20.0),
    ("NFEB222", 50.625, 19.375, 50.0, 51.25, 18.75, 20.0),
    ("NFEC333", 50.625, 20.625, 50.0, 51.25, 20.0, 21.25),
    ("NFEC332", 50.625, 21.875, 50.0, 51.25, 21.25, 22.5),
    ("NFEC344", 54.375, 20.625, 53.75, 55.0, 20.0, 21.25),
    ("NFEC341", 54.375, 21.875, 53.75, 55.0, 21.25, 22.5),
    ("NFEB242", 53.125, 16.875, 52.5, 53.75, 16.25, 17.5),
    ("NFEB231", 51.875, 16.875, 51.25, 52.5, 16.25, 17.5),
    ("NFEB224", 51.875, 18.125, 51.25, 52.5, 17.5, 18.75),
    ("NFEB311", 54.375, 14.375, 53.75, 55.0, 13.75, 15.0),
    ("NFEB312", 53.125, 14.375, 52.5, 53.75, 13.75, 15.0),
    ("NFEB243", 53.125, 15.625, 52.5, 53.75, 15.0, 16.25),
    ("NFEB244", 54.375, 15.625, 53.75, 55.0, 15.0, 16.25),
    ("NFEB111", 59.375, 19.375, 58.75, 60.0, 18.75, 20.0),
    ("NFEB112", 58.125, 19.375, 57.5, 58.75, 18.75, 20.0),
    ("NFEB113", 58.125, 18.125, 57.5, 58.75, 17.5, 18.75),
    ("NFEB121", 56.875, 19.375, 56.25, 57.5, 18.75, 20.0),
    ("NFEB122", 55.625, 19.375, 55.0, 56.25, 18.75, 20.0),
    ("NFEB123", 55.625, 18.125, 55.0, 56.25, 17.5, 18.75),
    ("NFEB124", 56.875, 18.125, 56.25, 57.5, 17.5, 18.75),
    ("NFEB131", 56.875, 16.875, 56.25, 57.5, 16.25, 17.5),
    ("NFEB132", 55.625, 16.875, 55.0, 56.25, 16.25, 17.5),
    ("NFEB133", 55.625, 15.625, 55.0, 56.25, 15.0, 16.25),
    ("NFEB134", 56.875, 15.625, 56.25, 57.5, 15.0, 16.25),
    ("NFEB142", 58.125, 16.875, 57.5, 58.75, 16.25, 17.5),
    ("NFEB143", 58.125, 15.625, 57.5, 58.75, 15.0, 16.25),
    ("NFEB412", 58.125, 14.375, 57.5, 58.75, 13.75, 15.0),
    ("NFEB413", 58.125, 13.125, 57.5, 58.75, 12.5, 13.75),
    ("NFEB421", 56.875, 14.375, 56.25, 57.5, 13.75, 15.0),
    ("NFEB422", 55.625, 14.375, 55.0, 56.25, 13.75, 15.0),
    ("NFEB423", 55.625, 13.125, 55.0, 56.25, 12.5, 13.75),
    ("NFEB424", 56.875, 13.125, 56.25, 57.5, 12.5, 13.75),
]
