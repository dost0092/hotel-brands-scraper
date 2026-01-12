import os

class HiltonConfig:
    START_URL = "https://www.hilton.com/en/locations/pet-friendly/"
    OUTPUT_FILE_CSV = "hilton_pet_friendly_hotels.csv"
    OUTPUT_FILE_JSON = "hilton_pet_friendly_hotels.json"
    STATE_FILE = "hilton_last_state.json"
    LOCATIONS_FILE = "hilton_locations.json"


class Hyattconfig:
    BASE_DIR = r"D:\Personal\KRUIZ\KRUIZ\hotel_brands_screper"

    JSON_DIR = os.path.join(BASE_DIR, "json", "hyatt")
    CSV_DIR = os.path.join(BASE_DIR, "csv", "hyatt")

    os.makedirs(JSON_DIR, exist_ok=True)
    os.makedirs(CSV_DIR, exist_ok=True)

    OUTPUT_JSON = os.path.join(JSON_DIR, "hyatt_hotels.json")
    OUTPUT_CSV = os.path.join(CSV_DIR, "hyatt_hotels.csv")
    CHECKPOINT_JSON = os.path.join(JSON_DIR, "hyatt_checkpoint.json")


class ihgconfig:
    BASE_DIR = r"D:\Personal\KRUIZ\KRUIZ\hotel_brands_screper"

    START_URL = "https://www.ihg.com/explore/pet-friendly-hotels"

    JSON_DIR = os.path.join(BASE_DIR, "json", "ihg")
    CSV_DIR = os.path.join(BASE_DIR, "csv", "ihg")

    os.makedirs(JSON_DIR, exist_ok=True)
    os.makedirs(CSV_DIR, exist_ok=True)

    CITY_CSV = os.path.join(CSV_DIR, "ihg_city_urls.csv")

    HOTEL_JSON = os.path.join(JSON_DIR, "ihg_hotels_output.json")
    CHECKPOINT_JSON = os.path.join(JSON_DIR, "ihg_checkpoint.json")

    HOTEL_CSV = os.path.join(CSV_DIR, "ihg_hotels_output.csv")

class MarriottConfig:
    BASE_DIR = r"D:\Personal\KRUIZ\KRUIZ\hotel_brands_screper"

    START_URL = "https://www.marriott.com/hotel-search.mi?filtersApplied=true&amenities=pet-friendly#/2/"

    JSON_DIR = os.path.join(BASE_DIR, "json", "marriott")
    CSV_DIR = os.path.join(BASE_DIR, "csv", "marriott")

    os.makedirs(JSON_DIR, exist_ok=True)
    os.makedirs(CSV_DIR, exist_ok=True)

    OUTPUT_FILE_JSON = os.path.join(JSON_DIR, "marriott_pet_friendly_hotels.json")
    OUTPUT_REGIONS_JSON = os.path.join(JSON_DIR, "marriott_pet_friendly_regions.json")
    STATE_FILE = os.path.join(JSON_DIR, "marriott_last_state.json")

    OUTPUT_FILE_CSV = os.path.join(CSV_DIR, "marriott_pet_friendly_hotels.csv")