class HiltonConfig:
    START_URL = "https://www.hilton.com/en/locations/pet-friendly/"
    OUTPUT_FILE_CSV = "hilton_pet_friendly_hotels.csv"
    OUTPUT_FILE_JSON = "hilton_pet_friendly_hotels.json"
    STATE_FILE = "hilton_last_state.json"
    LOCATIONS_FILE = "hilton_locations.json"


class Hyattconfig:
    OUTPUT_JSON = "hyatt_hotels.json"
    OUTPUT_CSV = "hyatt_hotels.csv"
    CHECKPOINT_JSON = "hyatt_checkpoint.json"


class ihgconfig:
    START_URL = "https://www.ihg.com/explore/pet-friendly-hotels"

    CITY_CSV = "ihg_city_urls.csv"             # stores city/category listing URLs
    HOTEL_JSON = "ihg_hotels_output.json"      # full output JSON
    HOTEL_CSV = "ihg_hotels_output.csv"        # full output CSV

    CHECKPOINT_JSON = "ihg_checkpoint.json"    # resume support: city index and hotel index

class MarriottConfig:
    START_URL = "https://www.marriott.com/hotel-search.mi?filtersApplied=true&amenities=pet-friendly#/2/"
    OUTPUT_FILE_CSV = "marriott_pet_friendly_hotels.csv"
    OUTPUT_FILE_JSON = "marriott_pet_friendly_hotels.json"
    OUTPUT_REGIONS_JSON = "marriott_pet_friendly_regions.json"
    STATE_FILE = "marriott_last_state.json"