# ihg_pet_hotels_scraper.py
import os
import re
import csv
import json
import time
import math
import random
from functools import wraps
from datetime import datetime
from urllib.parse import urlparse
from config import ihgconfig as config
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    ElementNotInteractableException,
    WebDriverException,
)

# =====================================================
# CONFIG
# =====================================================

START_URL = config.START_URL

CITY_CSV = config.CITY_CSV            
HOTEL_JSON = config.HOTEL_JSON     
HOTEL_CSV = config.HOTEL_CSV       
CHECKPOINT_JSON = "ihg_checkpoint.json"    

HEADLESS = False                             
DRIVER_TIMEOUT = 30
SLEEP_AFTER_LOAD = 1.5

RUN_ONLY_ONE_CITY = False                   
OVERWRITE = False                           

SAVE_EVERY_N_CITIES = 10                   

# Fields schema (order) — added city/state/country
FIELDS = [
    "hotel_code",
    "hotel_name",
    "address",
    "city",
    "state",
    "country",
    "phone",
    "rating",
    "description",
    "card_price",
    "overview_table_json",
    "pets_json",
    "parking_json",
    "amenities_json",
    "nearby_json",
    "airport_json",
    "is_pet_friendly",
    "last_updated"
]

# =====================================================
# UTILS
# =====================================================

def safe_text(el):
    try:
        return el.text.strip()
    except Exception:
        return None

def now_iso():
    return datetime.utcnow().isoformat()

def cleanup_price(text):
    if not text:
        return None
    return text.strip()

def extract_currency_from_card(card):
    try:
        cur = card.find_element(By.CSS_SELECTOR, ".cmp-card__hotel-price-currency")
        return safe_text(cur)
    except NoSuchElementException:
        return None

def get_hotel_code_from_url(url):
    if not url:
        return None
    try:
        path = urlparse(url).path.rstrip("/")
        parts = path.split("/")
        for p in parts:
            if len(p) == 5 and re.match(r"^[a-z0-9]{5}$", p, re.I):
                return p.lower()
        for p in reversed(parts):
            if p and p.lower() not in ("hoteldetail", "amenities"):
                return p.lower()
    except Exception:
        pass
    return None

def click_if_present(driver, wait, locator, extra_sleep=0.5):
    try:
        el = wait.until(EC.element_to_be_clickable(locator))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.2)
        el.click()
        time.sleep(extra_sleep)
        return True
    except (TimeoutException, ElementClickInterceptedException, ElementNotInteractableException):
        return False

def wait_presence(driver, wait, locator, timeout=None):
    try:
        if timeout:
            return WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))
        return wait.until(EC.presence_of_element_located(locator))
    except TimeoutException:
        return None

def wait_all_presence(driver, wait, locator, timeout=None):
    try:
        if timeout:
            return WebDriverWait(driver, timeout).until(EC.presence_of_all_elements_located(locator))
        return wait.until(EC.presence_of_all_elements_located(locator))
    except TimeoutException:
        return []

def ensure_dir(path):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

def soft_scroll(driver, steps=3):
    h = driver.execute_script("return document.body.scrollHeight || 2000;")
    for i in range(steps):
        y = int((i + 1) * h / steps)
        driver.execute_script(f"window.scrollTo(0, {y});")
        time.sleep(0.3)

def backoff_sleep(try_idx):
    # exponential backoff with jitter
    base = min(10, 2 ** try_idx)
    time.sleep(base + random.uniform(0.2, 0.8))

def normalize_json_field(obj_or_str):
    if obj_or_str is None:
        return None
    if isinstance(obj_or_str, str):
        return obj_or_str
    try:
        return json.dumps(obj_or_str, ensure_ascii=False)
    except Exception:
        return None

def parse_city_state_country(address_text):
    # Best-effort parse. IHG addresses often like:
    # "123 Main St, Austin, TX, United States"
    # or "123 Main St, Paris, France"
    if not address_text:
        return None, None, None
    parts = [p.strip() for p in re.split(r"[,|\n]+", address_text) if p.strip()]
    city = state = country = None
    # try last is country
    if parts:
        country = parts[-1]
    # try second last is state or region
    if len(parts) >= 2:
        state = parts[-2]
        # If state looks like a ZIP or street, shift logic
        if re.search(r"\d", state) and len(parts) >= 3:
            state = parts[-3]
    # try city earlier in the list
    if len(parts) >= 3:
        city = parts[-3]
        # Heuristics: if city contains digits or is too long, try previous
        if re.search(r"\d", city) and len(parts) >= 4:
            city = parts[-4]
    return city, state, country

# =====================================================
# RETRY DECORATOR
# =====================================================

def retryable(max_retries=3, refresh_on_fail=True):
    def deco(fn):
        @wraps(fn)
        def wrapper(self, *args, **kwargs):
            last_exc = None
            for attempt in range(max_retries):
                try:
                    return fn(self, *args, **kwargs)
                except (TimeoutException, WebDriverException, StaleElementReferenceException, ElementClickInterceptedException) as e:
                    last_exc = e
                    try:
                        if refresh_on_fail and hasattr(self, "driver"):
                            # Try a soft refresh
                            self.driver.execute_script("window.stop();")
                            self.driver.refresh()
                            time.sleep(1.5)
                    except Exception:
                        pass
                    backoff_sleep(attempt)
            # final try without refresh (optional)
            raise last_exc
        return wrapper
    return deco

# =====================================================
# BASE DRIVER
# =====================================================

class BaseScraper:
    def __init__(self, headless=True, timeout=30):
        self.timeout = timeout
        self.driver = self._init_driver(headless)
        self.wait = WebDriverWait(self.driver, self.timeout)

    def _init_driver(self, headless):
        options = uc.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--lang=en-US,en;q=0.9")
        options.add_argument("--start-maximized")
        driver = uc.Chrome(options=options)
        driver.set_page_load_timeout(60)
        return driver

    def open(self, url):
        self.driver.get(url)

    def quit(self):
        try:
            self.driver.quit()
        except Exception:
            pass

# =====================================================
# CHECKPOINTING
# =====================================================

class CheckpointManager:
    def __init__(self, path):
        self.path = path
        self.state = {
            "city_index": 0,        # index into cities list
            "hotel_index": 0,       # index within current city's hotels list
        }
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self.state = json.load(f)
            except Exception:
                pass

    def save(self):
        ensure_dir(self.path)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)

    def set_city(self, idx):
        self.state["city_index"] = idx
        self.state["hotel_index"] = 0
        self.save()

    def set_hotel(self, idx):
        self.state["hotel_index"] = idx
        self.save()

    def clear(self):
        self.state = {"city_index": 0, "hotel_index": 0}
        self.save()

# =====================================================
# STEP 1 — CITY URL SCRAPER (from main Explore page)
# =====================================================

class IHGCitiesScraper(BaseScraper):
    @retryable(max_retries=3)
    def scrape_city_urls(self):
        self.open(START_URL)
        self._dismiss_language_popover_if_any()
        self._accept_cookies_if_present()

        el = wait_presence(
            self.driver, self.wait,
            (By.CSS_SELECTOR, "ul.cmp-list a.cmp-list__item-link")
        )
        time.sleep(SLEEP_AFTER_LOAD)

        links = self.driver.find_elements(By.CSS_SELECTOR, "ul.cmp-list a.cmp-list__item-link")
        seen = set()
        cities = []
        for el in links:
            url = el.get_attribute("href")
            name = el.text.strip()
            if not url or url in seen:
                continue
            # keep pet friendly related or destination listing pages
            if "ihg.com" in url and ("hotels" in url or "/explore/" in url or "/destinations" in url or "pet" in url.lower()):
                seen.add(url)
                cities.append({"city_name": name, "city_url": url})
        return cities

    def _accept_cookies_if_present(self):
        selectors = [
            'button#onetrust-accept-btn-handler',
            'button[aria-label="Accept all"]',
            'button[aria-label="Accept All"]',
            'button:has(span:contains("Accept"))',
        ]
        for sel in selectors:
            try:
                btns = self.driver.find_elements(By.CSS_SELECTOR, sel)
                if btns:
                    self.driver.execute_script("arguments[0].click();", btns[0])
                    time.sleep(0.5)
                    return True
            except Exception:
                pass
        return False

    def _dismiss_language_popover_if_any(self):
        try:
            # Attempt to click NO or close on language prompt
            candidates = self.driver.find_elements(By.XPATH, '//button[normalize-space()="NO" or normalize-space()="No" or normalize-space()="No thanks"]')
            if candidates:
                self.driver.execute_script("arguments[0].click();", candidates[0])
                time.sleep(0.4)
        except Exception:
            pass

# =====================================================
# STEP 2 — HOTEL SCRAPER
# =====================================================

class IHGHotelScraper(BaseScraper):

    def restart_browser(self, headless=True):
        # Close current and reopen a fresh driver to avoid memory/leaks
        try:
            self.quit()
        except Exception:
            pass
        self.driver = self._init_driver(headless)
        self.wait = WebDriverWait(self.driver, self.timeout)

    def _accept_cookies_if_present(self):
        possible_selectors = [
            'button#onetrust-accept-btn-handler',
            'button[aria-label="Accept all"]',
            'button[aria-label="Accept All"]',
        ]
        for sel in possible_selectors:
            try:
                btns = self.driver.find_elements(By.CSS_SELECTOR, sel)
                if btns:
                    btn = btns[0]
                    self.driver.execute_script("arguments[0].click();", btn)
                    time.sleep(0.5)
                    return True
            except Exception:
                pass
        return False

    @retryable(max_retries=3)
    def scrape_city(self, city, resume_hotel_index=0):
        self.open(city["city_url"])
        self._accept_cookies_if_present()
        soft_scroll(self.driver, steps=3)

        # Wait for hotel list container (best-effort)
        wait_presence(self.driver, self.wait, (By.ID, "hotelList"))
        time.sleep(SLEEP_AFTER_LOAD)

        # Ensure list items present
        cards = self.driver.find_elements(By.CSS_SELECTOR, "#hotelList > div > ul > li")
        hotels = []

        # If no cards found, try a more generic pattern
        if not cards:
            cards = self.driver.find_elements(By.CSS_SELECTOR, "li .cmp-card__title-link, li .cmp-card")

        for idx, card in enumerate(cards, start=0):
            if idx < resume_hotel_index:
                continue
            try:
                # NAME + URL
                try:
                    name_el = card.find_element(By.CSS_SELECTOR, "a.cmp-card__title-link")
                except NoSuchElementException:
                    # fallback: any anchor with title-ish role
                    anchors = card.find_elements(By.CSS_SELECTOR, "a")
                    name_el = anchors[0] if anchors else None

                if not name_el:
                    continue

                hotel_name = safe_text(name_el)
                hotel_url = name_el.get_attribute("href") or ""

                # ADDRESS
                address = None
                try:
                    addr_el = card.find_element(By.CSS_SELECTOR, "address")
                    address = safe_text(addr_el)
                except NoSuchElementException:
                    # fallback: small or p labels containing address-like content
                    for sel in ["p", "small", ".cmp-card__address"]:
                        try:
                            els = card.find_elements(By.CSS_SELECTOR, sel)
                            for e in els:
                                txt = safe_text(e)
                                if txt and re.search(r"\d", txt) and "," in txt:
                                    address = txt
                                    break
                            if address:
                                break
                        except Exception:
                            pass

                # CARD AMENITIES (list of labels)
                card_amenities = []
                for li in card.find_elements(By.CSS_SELECTOR, ".cmp-amenity-list .cmp-amenity-list__item .cmp-image__title"):
                    label = safe_text(li)
                    if label:
                        card_amenities.append(label)

                # PRICE + CURRENCY
                price_value = None
                try:
                    pv = card.find_element(By.CSS_SELECTOR, ".cmp-card__hotel-price-value")
                    price_value = cleanup_price(safe_text(pv))
                except NoSuchElementException:
                    pass

                currency = extract_currency_from_card(card)

                # RATING (from card, if available)
                rating = None
                try:
                    r_el = card.find_element(By.CSS_SELECTOR, ".cmp-card__guest-reviews .cmp-card__rating-count")
                    rating = safe_text(r_el)
                except NoSuchElementException:
                    pass

                city_name = city.get("city_name")
                cty, st, cn = parse_city_state_country(address or "")

                hotel_record = {
                    "hotel_code": get_hotel_code_from_url(hotel_url),
                    "hotel_name": hotel_name,
                    "address": address,
                    "city": cty or city_name,
                    "state": st,
                    "country": cn,
                    "phone": None,
                    "rating": rating,
                    "description": None,
                    "card_price": f"{price_value} {currency}".strip() if price_value else None,
                    "overview_table_json": None,
                    "pets_json": None,
                    "parking_json": None,
                    "amenities_json": json.dumps(card_amenities, ensure_ascii=False) if card_amenities else None,
                    "nearby_json": None,
                    "airport_json": None,
                    "is_pet_friendly": None,
                    "last_updated": now_iso(),
                    "_detail_url": hotel_url,             # internal helper
                    "_city": city_name,                   # internal helper
                }

                # Visit detail page in new tab to extract deeper data
                detail = self.scrape_hotel_detail(hotel_url)
                hotel_record.update(detail)

                # Normalize to required JSON strings for *_json fields if dict/list
                for key in ["overview_table_json", "pets_json", "parking_json", "amenities_json", "nearby_json", "airport_json"]:
                    hotel_record[key] = normalize_json_field(hotel_record.get(key))

                # Coerce is_pet_friendly to string "true"/"false"
                if isinstance(hotel_record.get("is_pet_friendly"), bool):
                    hotel_record["is_pet_friendly"] = "true" if hotel_record["is_pet_friendly"] else "false"

                # Remove helpers
                hotel_record.pop("_detail_url", None)
                hotel_record.pop("_city", None)

                hotels.append(hotel_record)

            except (NoSuchElementException, StaleElementReferenceException, WebDriverException):
                continue

        return hotels

    @retryable(max_retries=3)
    def scrape_hotel_detail(self, url):
        out = {
            "phone": None,
            "description": None,
            "overview_table_json": None,
            "pets_json": None,
            "parking_json": None,
            "amenities_json": None,  # if we find a more complete list than card level
            "nearby_json": None,
            "airport_json": None,
            "is_pet_friendly": None,
        }
        if not url:
            return out

        main = self.driver.current_window_handle
        # Open in new tab
        self.driver.switch_to.new_window('tab')
        opened = False
        try:
            self.open(url)
            opened = True
        except WebDriverException:
            # If navigation fails, close tab and return partial
            try:
                self.driver.close()
                self.driver.switch_to.window(main)
            except Exception:
                pass
            return out

        try:
            self._accept_cookies_if_present()
            self._expand_description_if_present()
            soft_scroll(self.driver, steps=4)

            # Extract description
            out["description"] = self._extract_description_text()

            # Extract highlight amenity icons section
            highlights = self._extract_highlights_section()
            if highlights:
                out["amenities_json"] = highlights

            # Extract phone
            phone = self._extract_phone()
            if phone:
                out["phone"] = phone

            # Open amenities page (various button cases)
            amenities_detail_data = self._open_amenities_page_and_scrape()
            if amenities_detail_data:
                if amenities_detail_data.get("amenities"):
                    out["amenities_json"] = amenities_detail_data["amenities"]
                if amenities_detail_data.get("parking"):
                    out["parking_json"] = amenities_detail_data["parking"]
                if amenities_detail_data.get("overview"):
                    out["overview_table_json"] = amenities_detail_data["overview"]
                if amenities_detail_data.get("nearby"):
                    out["nearby_json"] = amenities_detail_data["nearby"]
                if amenities_detail_data.get("airport"):
                    out["airport_json"] = amenities_detail_data["airport"]
                if amenities_detail_data.get("phone") and not out.get("phone"):
                    out["phone"] = amenities_detail_data["phone"]

            # Pet policy variants — explicit button or inline text
            pets_data = self._open_pet_policy_if_available()
            if not pets_data:
                pets_data = self._scrape_faq_pet_policy()
            if not pets_data:
                # Inline pet policy paragraphs (cases you provided)
                pets_data = self._scrape_inline_pet_policy_blocks()
            if pets_data:
                out["pets_json"] = pets_data
                out["is_pet_friendly"] = True
            else:
                out["is_pet_friendly"] = self._infer_pet_friendly(out)
            print(f"Scraped pet policy: {pets_data}")
            return out

        finally:
            # Close detail tab and return to main
            try:
                if opened:
                    self.driver.close()
                    self.driver.switch_to.window(main)
            except Exception:
                pass

    def _expand_description_if_present(self):
        try:
            self.driver.execute_script("window.scrollTo(0, 400);")
            time.sleep(0.4)
            more_links = self.driver.find_elements(By.CSS_SELECTOR, "a.morelink, a.moreLink, a.read-more, a.readmore, button.read-more")
            for ml in more_links:
                txt = (ml.text or "").strip().lower()
                if any(k in txt for k in ["read more", "show more", "see more"]):
                    self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", ml)
                    time.sleep(0.2)
                    try:
                        ml.click()
                    except Exception:
                        self.driver.execute_script("arguments[0].click();", ml)
                    time.sleep(0.5)
                    break
        except Exception:
            pass

    def _extract_description_text(self):
        candidates = [
            "div.hotel-description, div.description, .hotel-overview, .vx-description, .property-description",
            "section#overview, section.overview",
            ".cmp-text, .ihg-copy, .content-copy, .cmp-teaser__description",
        ]
        for sel in candidates:
            try:
                blocks = self.driver.find_elements(By.CSS_SELECTOR, sel)
                texts = []
                for b in blocks:
                    t = safe_text(b)
                    if t and len(t) > 120:
                        texts.append(t)
                if texts:
                    return max(texts, key=len)
            except Exception:
                continue

        try:
            ps = self.driver.find_elements(By.CSS_SELECTOR, "p")
            best = ""
            for p in ps:
                t = safe_text(p) or ""
                if len(t) > len(best):
                    best = t
            return best if best else None
        except Exception:
            return None

    def _extract_highlights_section(self):
        try:
            # Try known containers
            containers = self.driver.find_elements(By.CSS_SELECTOR, ".vx-highlight-items, .cmp-amenity-list")
            titles = []
            for c in containers:
                items = c.find_elements(By.CSS_SELECTOR, ".vx-highlight-item .amenity-title, .cmp-amenity-list__item .cmp-image__title")
                for i in items:
                    t = safe_text(i)
                    if t:
                        titles.append(t)
            return titles if titles else None
        except Exception:
            return None
    def _scrape_faq_pet_policy(self):
        """
        Clicks pet-related FAQ accordion questions and extracts the answer text.
        This is the PRIMARY source of pet policy.
        """
        try:
            pet_answers = []

            accordion_items = self.driver.find_elements(
                By.CSS_SELECTOR, ".cmp-accordion__item"
            )

            for item in accordion_items:
                try:
                    button = item.find_element(
                        By.CSS_SELECTOR, "button.cmp-accordion__button"
                    )

                    question = safe_text(button)
                    if not question:
                        continue

                    # ✅ Only pet-related questions
                    if not re.search(r"\bpet|pets|dog|cat\b", question, re.I):
                        continue

                    # ✅ Expand if collapsed
                    expanded = button.get_attribute("aria-expanded") == "true"
                    if not expanded:
                        self.driver.execute_script(
                            "arguments[0].scrollIntoView({block:'center'});", button
                        )
                        time.sleep(0.2)
                        try:
                            button.click()
                        except Exception:
                            self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(0.4)

                    # ✅ Read the panel
                    panels = item.find_elements(
                        By.CSS_SELECTOR, ".cmp-accordion__panel"
                    )

                    for panel in panels:
                        txt = safe_text(panel)
                        if txt and re.search(r"\bpet|pets|dog|cat\b", txt, re.I):
                            pet_answers.append(txt)

                except Exception:
                    continue

            if pet_answers:
                return {"policy": "\n\n".join(dict.fromkeys(pet_answers))}

            return None

        except Exception:
            return None

    def _extract_phone(self):
        try:
            tel_links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href^="tel:"]')
            for a in tel_links:
                href = a.get_attribute("href") or ""
                num = href.replace("tel:", "").strip()
                if num:
                    return num
        except Exception:
            pass
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            m = re.search(r"(\+?\d[\d\-\(\) \.]{7,}\d)", body_text)
            if m:
                return m.group(1).strip()
        except Exception:
            pass
        return None

    def _open_amenities_page_and_scrape(self):
        # Support “View all amenities”, “VIEW ALL AMENITIES”, “Explore all amenities”
        try:
            btn = None
            buttons = self.driver.find_elements(By.CSS_SELECTOR, 'a.cmp-button, a.cmp-teaser__action-link, a[aria-label]')
            for b in buttons:
                txt = (b.text or "").strip().lower()
                aria = (b.get_attribute("aria-label") or "").strip().lower()
                if any(kw in txt for kw in ["view all amenities", "view all  amenities", "explore all amenities"]) or \
                   any(kw in aria for kw in ["view all amenities", "explore all amenities"]):
                    btn = b
                    break

            if not btn:
                return None

            href = btn.get_attribute("href")
            if not href:
                return None

            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.2)
            try:
                btn.click()
            except Exception:
                self.driver.execute_script("arguments[0].click();", btn)

            time.sleep(0.8)
            wait_presence(self.driver, self.wait, (By.TAGNAME, "body"))
        except Exception:
            # small selector typo fallback
            try:
                wait_presence(self.driver, self.wait, (By.TAG_NAME, "body"))
            except Exception:
                pass

        try:
            time.sleep(0.8)
            soft_scroll(self.driver, steps=3)

            data = {
                "amenities": self._scrape_amenities_list_from_page(),
                "parking": self._scrape_parking_from_page(),
                "overview": self._scrape_overview_table_from_page(),
                "nearby": self._scrape_nearby_from_page(),
                "airport": self._scrape_airport_from_page(),
                "phone": self._extract_phone()
            }

            # Navigate back
            try:
                self.driver.back()
            except Exception:
                pass
            time.sleep(0.8)

            if not any([data.get("amenities"), data.get("parking"), data.get("overview"), data.get("nearby"), data.get("airport")]):
                return None

            return data

        except Exception:
            try:
                self.driver.back()
            except Exception:
                pass
            return None

    def _scrape_amenities_list_from_page(self):
        selectors = [
            ".amenities-list li",
            ".cmp-amenity-list .cmp-image__title",
            ".cmp-amenity-list__item .cmp-image__title",
            ".amenities .amenity, .amenities li",
            '[data-component="amenities"] li',
            ".cmp-list__item .cmp-image__title",
        ]
        items = []
        for sel in selectors:
            try:
                els = self.driver.find_elements(By.CSS_SELECTOR, sel)
                for e in els:
                    t = safe_text(e)
                    if t and t not in items:
                        items.append(t)
            except Exception:
                continue
        return items if items else None

    def _scrape_parking_from_page(self):
        text = self._collect_section_text(["parking", "valet", "self-parking", "self parking"])
        if text:
            return {"parking_info": text}
        return None

    def _scrape_overview_table_from_page(self):
        data = {}
        try:
            dts = self.driver.find_elements(By.CSS_SELECTOR, "dl dt")
            dds = self.driver.find_elements(By.CSS_SELECTOR, "dl dd")
            if dts and dds and len(dts) == len(dds):
                for dt, dd in zip(dts, dds):
                    k = safe_text(dt)
                    v = safe_text(dd)
                    if k and v:
                        data[k] = v

            if not data:
                # generic k/v rows
                tables = self.driver.find_elements(By.CSS_SELECTOR, ".table, .overview, .kv, .grid, table")
                for r in tables:
                    labels = r.find_elements(By.CSS_SELECTOR, "th, .label, .key")
                    vals = r.find_elements(By.CSS_SELECTOR, "td, .value")
                    if labels and vals and len(labels) == len(vals):
                        for k_el, v_el in zip(labels, vals):
                            k = safe_text(k_el)
                            v = safe_text(v_el)
                            if k and v:
                                data[k] = v
        except Exception:
            pass
        return data if data else None

    def _scrape_nearby_from_page(self):
        text = self._collect_section_text(["nearby", "attractions", "points of interest"])
        if text:
            lines = [ln.strip(" -•\t") for ln in text.splitlines() if ln.strip()]
            return lines if lines else None
        return None

    def _scrape_airport_from_page(self):
        text = self._collect_section_text(["airport", "airports", "shuttle"])
        if text:
            lines = [ln.strip(" -•\t") for ln in text.splitlines() if ln.strip()]
            return lines if lines else None
        return None

    def _collect_section_text(self, keywords):
        try:
            sections = self.driver.find_elements(By.CSS_SELECTOR, "section, .section, .cmp-section, .content-section, .accordion, .accordion-item, .cmp-teaser__description")
            texts = []
            for sec in sections:
                full_txt = (safe_text(sec) or "")
                if not full_txt:
                    continue
                low = full_txt.lower()
                if any(k in low for k in keywords):
                    if full_txt not in texts:
                        texts.append(full_txt)
            if not texts:
                body = self.driver.find_element(By.TAG_NAME, "body").text
                low = body.lower()
                for k in keywords:
                    if k in low:
                        idx = low.find(k)
                        snippet = body[max(0, idx-500): idx+700]
                        texts.append(snippet)
                        break
            if texts:
                return max(texts, key=len)
        except Exception:
            pass
        return None

    def _open_pet_policy_if_available(self):
        try:
            links = self.driver.find_elements(By.CSS_SELECTOR, 'a.cmp-teaser__action-link.cmp-button, a.cmp-button, a[aria-label]')
            pet_link = None
            for a in links:
                txt = (a.text or "").strip().lower()
                aria = (a.get_attribute("aria-label") or "").strip().lower()
                if any(k in txt for k in ["view pet policy", "pet policy"]) or any(k in aria for k in ["view pet policy", "pet policy"]):
                    pet_link = a
                    break
            if not pet_link:
                return None

            href = pet_link.get_attribute("href")
            if not href:
                return None

            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", pet_link)
            time.sleep(0.2)
            try:
                pet_link.click()
            except Exception:
                self.driver.execute_script("arguments[0].click();", pet_link)

            time.sleep(0.8)
            wait_presence(self.driver, self.wait, (By.TAG_NAME, "body"))
            time.sleep(0.6)

            policy = self._collect_section_text(["pet", "pets", "pet policy", "dog", "cat"])

            # Navigate back
            self.driver.back()
            time.sleep(0.8)

            if policy:
                return {"policy": policy}
            return None

        except Exception:
            try:
                self.driver.back()
            except Exception:
                pass
            return None

    def _scrape_inline_pet_policy_blocks(self):
        # Handle cases:
        # <div class="cmp-teaser__description rte" ...><p>Pets are allowed ...</p></div>
        # And your XPath-like patterns with spans containing pet paragraphs
        try:
            # Primary: any cmp-teaser__description.rte paragraph mentioning pet
            blocks = self.driver.find_elements(By.CSS_SELECTOR, ".cmp-teaser__description.rte, .cmp-teaser__description")
            texts = []
            for b in blocks:
                ps = b.find_elements(By.CSS_SELECTOR, "p")
                for p in ps:
                    t = safe_text(p)
                    if t and re.search(r"\bpet|pets|dog|cat\b", t, re.I):
                        texts.append(t)

            # Secondary: generic spans or divs with pet text
            spans = self.driver.find_elements(By.CSS_SELECTOR, "span, div")
            for sp in spans:
                t = safe_text(sp)
                if t and len(t) > 20 and re.search(r"\bpet|pets|dog|cat\b", t, re.I):
                    texts.append(t)

            # Tertiary: probe your provided absolute XPaths safely
            # Note: absolute XPaths are fragile; we try both and ignore failures
            for xp in [
                '//*[@id="uuid207930900"]/div/div/div[2]/div/div[4]/span',
                '//*[@id="uuid207930900"]/div/div/div[2]/div/div[4]/span/div/p',
                '//*[@id="uuid367772062"]/div/div[2]/div/div[2]/div/div[4]/span',
            ]:
                try:
                    els = self.driver.find_elements(By.XPATH, xp)
                    for e in els:
                        t = safe_text(e)
                        if t and re.search(r"\bpet|pets|dog|cat\b", t, re.I):
                            texts.append(t)
                except Exception:
                    pass

            # Deduplicate and join
            texts = [t for t in {t for t in texts if t}]
            if texts:
                return {"policy": "\n\n".join(texts)}
            return None
        except Exception:
            return None

    def _infer_pet_friendly(self, detail_dict):
        if detail_dict.get("pets_json"):
            return True
        desc = (detail_dict.get("description") or "").lower()
        if any(w in desc for w in ["pet-friendly", "pets allowed", "pet friendly", "pet-friendly hotel", "pet policy"]):
            return True
        ams = detail_dict.get("amenities_json")
        if isinstance(ams, list):
            hay = " ".join(ams).lower()
            if any(w in hay for w in ["pet", "pets allowed", "pet-friendly", "pet friendly", "pets welcome"]):
                return True
        elif isinstance(ams, str):
            try:
                lst = json.loads(ams)
                if isinstance(lst, list):
                    hay = " ".join(lst).lower()
                    if any(w in hay for w in ["pet", "pets allowed", "pet-friendly", "pet friendly", "pets welcome"]):
                        return True
            except Exception:
                pass
        return False

# =====================================================
# STORAGE HELPERS (Resume support)
# =====================================================

def load_or_create_city_csv():
    if os.path.exists(CITY_CSV):
        with open(CITY_CSV, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
            if rows:
                return rows

    print("City CSV not found or empty — scraping cities...")
    scraper = IHGCitiesScraper(headless=HEADLESS, timeout=DRIVER_TIMEOUT)
    try:
        cities = scraper.scrape_city_urls()
    finally:
        scraper.quit()

    ensure_dir(CITY_CSV)
    with open(CITY_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["city_name", "city_url"])
        writer.writeheader()
        writer.writerows(cities)

    return cities

def load_existing_output():
    existing = {}
    if os.path.exists(HOTEL_JSON) and not OVERWRITE:
        try:
            with open(HOTEL_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                for d in data:
                    key = (d.get("hotel_code"), d.get("hotel_name"))
                    existing[key] = d
        except Exception:
            pass
    return existing

def append_or_merge(hotels, existing_map):
    result_map = dict(existing_map)
    for h in hotels:
        key = (h.get("hotel_code"), h.get("hotel_name"))
        if key in result_map and not OVERWRITE:
            # merge minimal: keep earliest non-null; update if new has data the old misses
            merged = dict(result_map[key])
            for k, v in h.items():
                if merged.get(k) in [None, "", "null"] and v not in [None, "", "null"]:
                    merged[k] = v
            result_map[key] = merged
            continue
        result_map[key] = h
    return list(result_map.values())

def save_outputs(records):
    if not records:
        return
    # JSON
    ensure_dir(HOTEL_JSON)
    with open(HOTEL_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    # CSV
    ensure_dir(HOTEL_CSV)
    with open(HOTEL_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for r in records:
            row = {k: r.get(k) for k in FIELDS}
            writer.writerow(row)

# =====================================================
# MAIN
# =====================================================

def main():
    checkpoint = CheckpointManager(CHECKPOINT_JSON)

    cities = load_or_create_city_csv()

    if RUN_ONLY_ONE_CITY:
        # if resuming and within first city, keep resume index; else slice to that city
        start_city_idx = checkpoint.state.get("city_index", 0)
        if start_city_idx > 0:
            cities = cities[start_city_idx:start_city_idx+1]
        else:
            cities = cities[:1]

    existing_map = load_existing_output()
    all_hotels = list(existing_map.values())

    # Create a scraper instance; we will restart after each city
    hotel_scraper = IHGHotelScraper(headless=HEADLESS, timeout=DRIVER_TIMEOUT)

    processed_cities_since_save = 0

    try:
        start_city_idx = checkpoint.state.get("city_index", 0)
        start_hotel_idx = checkpoint.state.get("hotel_index", 0)

        for i, city in enumerate(cities):
            global_city_index = i
            if not RUN_ONLY_ONE_CITY:
                global_city_index = i
            else:
                # in RUN_ONLY_ONE_CITY mode, adapt the index to global checkpoint
                global_city_index = start_city_idx + i

            # Skip until we reach checkpoint's city index
            if global_city_index < start_city_idx:
                continue

            print(f"Scraping city [{global_city_index+1}/{len(cities)}]: {city.get('city_name')} -> {city.get('city_url')}")
            checkpoint.set_city(global_city_index)

            # Resume hotel index only for the first city to resume; else 0
            resume_hotel_index = start_hotel_idx if global_city_index == start_city_idx else 0

            try:
                hotels = hotel_scraper.scrape_city(city, resume_hotel_index=resume_hotel_index)
            except Exception as e:
                print(f"Error scraping city {city.get('city_name')}: {e}")
                hotels = []

            # After scrape, reset hotel checkpoint
            checkpoint.set_hotel(0)

            merged = append_or_merge(hotels, {(h.get('hotel_code'), h.get('hotel_name')): h for h in all_hotels})
            all_hotels = merged

            # Save after each city
            save_outputs(all_hotels)
            processed_cities_since_save += 1
            print(f"Saved total hotels so far: {len(all_hotels)}")

            # Additionally save after every SAVE_EVERY_N_CITIES (already saved, but this keeps your requirement explicit)
            if processed_cities_since_save >= SAVE_EVERY_N_CITIES:
                save_outputs(all_hotels)
                processed_cities_since_save = 0

            # Restart browser to keep it fresh for all-night runs
            print("Restarting browser to keep session fresh...")
            hotel_scraper.restart_browser(headless=HEADLESS)

        print(f"Done. Total hotels: {len(all_hotels)}")
        print(f"JSON: {os.path.abspath(HOTEL_JSON)}")
        print(f"CSV:  {os.path.abspath(HOTEL_CSV)}")

        # Clear checkpoint when fully done
        checkpoint.clear()

    finally:
        hotel_scraper.quit()

if __name__ == "__main__":
    main()