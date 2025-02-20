import sys, os, json, time, threading, io
import pycurl, certifi
import unicodedata, re
import random
from datetime import datetime, timedelta, timezone

import signal
import psycopg2
from typing import List, Dict, Tuple


fileName="product_data.json"

# Configuration
MAX_DOWNLOAD_ATTEMPTS = 2
BASE_DIR = os.path.join(os.getcwd(), f"/home/iheb/Desktop/IMG2/{fileName}/")
IMAGE_EXTENSIONS_REGEX = re.compile(r"(?i)(\.jpg|\.jpeg|\.png|\.gif)")

MAX_PRODUCTS_PER_ITER = 5
SUCCESS_THRESHOLD = 0.9  
MAX_THREADS = 5  # Nombre maximum de threads pour le téléchargement d'images

DB_CONFIG = {
    "dbname": "new_db2",
    "user": "new_user2",
    "password": "nazir",
    "host": "localhost",
    "port": 5432
}

# Database Connection
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()
def close_db(conn):
    try:
        conn.close()
        print("[INFO] Database connection closed.")
    except Exception as e:
        print(f"[ERROR] Failed to close database connection: {e}")


def handle_exit(signal, frame):
    print("\n[INFO] Gracefully shutting down... Saving progress if needed.")
    close_db(conn) 
    sys.exit(0)

# Attach signal handler
signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

def clock_ms() -> float:
    return time.monotonic() * 1000.

def nowutc():
    return datetime.now(timezone.utc)

def sleep_mcs(dt_mcs):
    threading.Event().wait(dt_mcs / 1.e6)

def create_image_path(img_url: str, product_id: str, index: int, category: str) -> str:
    match = IMAGE_EXTENSIONS_REGEX.search(img_url)
    ext = match.group(0) if match else ".jpg"
    filename = f"{product_id}_{index}{ext}"
    return os.path.join(BASE_DIR, product_id, category, filename)

def insert_product(product_id: str, product_details: Dict, conn):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO mproducts (product_id, product_details, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_id) DO NOTHING
                """,
                (product_id, json.dumps(product_details), "pending")
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to insert product {product_id}: {e}")

def insert_images(product_id: str, category: str, success_images: List[Tuple], conn):
    try:
        with conn.cursor() as cur:
            for item in success_images:
                if len(item) == 2:  
                    original_url, new_path = item
                else:
                    print(f"[WARNING] Skipping invalid entry: {item}")
                    continue

                cur.execute(
                    """
                    INSERT INTO mimages (product_id, original_url, category, new_location)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (product_id, original_url, category, new_path)
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to insert images for {product_id}: {e}")

def insert_sku(product_id: str, sku_data: List[Dict], conn):
    try:
        with conn.cursor() as cur:
            for sku in sku_data:
                cur.execute(
                    """
                    INSERT INTO msku (sku_id, product_id, sku_props, sku_image, original_price, quantity, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sku_id) DO NOTHING
                    """,
                    (
                        sku.get("skuId"),
                        product_id,
                        json.dumps(sku.get("skuProps", [])),
                        sku["skuImage"].get("RU", ""),
                        sku.get("originalPrice", 0),
                        sku.get("quantity", 0),
                        sku.get("status", "active")
                    )
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to insert SKU for {product_id}: {e}")

def update_product_status(product_id: str, status: str, conn):
    """Updates the product status in the mproductq table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE mproducts SET status = %s WHERE product_id = %s",
                (status, product_id)
            )
        conn.commit()
        print(f"[UPDATED] Product {product_id} status set to {status}")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to update product {product_id} status: {e}")

def download_image(img_url: str, file_path: str) -> bool:
    try:
        curl = pycurl.Curl()
        curl.setopt(curl.URL, img_url)
        curl.setopt(pycurl.CAINFO, certifi.where())
        with open(file_path, "wb") as f:
            curl.setopt(curl.WRITEDATA, f)
            curl.perform()
        curl.close()
        return True
    except Exception as e:
        print(f"[ERROR] Failed to download {img_url}: {e}")
        return False
    finally:
        curl.close()

def download_images(image_urls: List[str], product_id: str, category: str) -> Tuple[int, List[Tuple[str, str]], List[int], List]:
    image_paths = []
    success_count = 0
    success_images = []
    aiimg_err_retry = []
    hiimg_err_final = []  

    product_folder = os.path.join(BASE_DIR, product_id, category)
    os.makedirs(product_folder, exist_ok=True)

    def download_thread(img_url, file_path, idx):
        nonlocal success_count
        if download_image(img_url, file_path):
            success_count += 1
            success_images.append((img_url, file_path))
            print(f"[SUCCESS] {img_url} -> {file_path}")
        else:
            aiimg_err_retry.append(idx)

    threads = []
    for idx, img_url in enumerate(image_urls):
        file_path = create_image_path(img_url, product_id, idx + 1, category)
        image_paths.append(file_path)
        full_url = "https:" + img_url if img_url.startswith("//") else img_url
        thread = threading.Thread(target=download_thread, args=(full_url, file_path, idx))
        threads.append(thread)
        thread.start()
        if len(threads) >= MAX_THREADS:
            for t in threads:
                t.join()
            threads = []

    for t in threads:
        t.join()

    return success_count, success_images, aiimg_err_retry, hiimg_err_final

'''def process_product(product, conn):
    product_id = product.get("productId")
    main_images = product.get("mainImages", {}).get("RU", [])
    sku_data = product.get("sku", [])
    sku_images = [sku.get("skuImage", {}).get("RU") for sku in sku_data if sku.get("skuImage", {}).get("RU")]

    if not product_id:
        print(f"[ERROR] Missing 'productId' in product: {product}")
        return

    update_product_status(product_id, "processing", conn)

    try:
        insert_product(product_id, product, conn)
        insert_sku(product_id, sku_data, conn)

        # For overall product ratio calculation
        product_total_images = 0
        product_total_downloaded = 0

        for category, images in [("main", main_images), ("sku", sku_images)]:
            if not images:
                print(f"[INFO] No {category} images found for product {product_id}")
                continue

            original_images = images[:]  
            category_total = len(original_images)
            downloaded_this_category = 0
            current_images = images[:]  
            attempt = 1

            while attempt <= MAX_DOWNLOAD_ATTEMPTS and current_images:
                print(f"[INFO] Attempt {attempt} for {category} images of product {product_id}")
                dl_count, success_images, err_retry, hiimg_err_final = download_images(current_images, product_id, category)
                
                if success_images:
                    insert_images(product_id, category, success_images, conn)
                downloaded_this_category += dl_count

                if not err_retry:
                    break  # All images in current_images downloaded successfully
                else:
                    # Prepare for next retry: only try the images that failed in this attempt
                    current_images = [current_images[i] for i in err_retry]
                attempt += 1

            print(f"[INFO] {category} images: Downloaded {downloaded_this_category}/{category_total}")
            product_total_images += category_total
            product_total_downloaded += downloaded_this_category

        success_ratio = product_total_downloaded / product_total_images if product_total_images > 0 else 0
        print(f"[INFO] Overall success ratio for product {product_id}: {success_ratio:.2%}")

        if success_ratio >= SUCCESS_THRESHOLD:
            update_product_status(product_id, "downloaded", conn)
        else:
            update_product_status(product_id, "completed", conn)
    except Exception as e:
        print(f"[ERROR] Processing failed for product {product_id}: {e}")
        update_product_status(product_id, "failed", conn)
        '''
def process_product(product, conn):
    product_id = product.get("productId")
    if not product_id:
        print(f"[ERROR] Missing 'productId' in product: {product}")
        return

    update_product_status(product_id, "processing", conn)
    start_time = time.time()  # Début du traitement
    MAX_PROCESSING_TIME = 100 # 600=10 minutes en secondes

    try:
        insert_product(product_id, product, conn)
        sku_data = product.get("sku", [])
        insert_sku(product_id, sku_data, conn)

        product_total_images = 0
        product_total_downloaded = 0

        for category, images in [("main", product.get("mainImages", {}).get("RU", [])), 
                                 ("sku", [sku.get("skuImage", {}).get("RU") for sku in sku_data if sku.get("skuImage", {}).get("RU")])]:
            if not images:
                continue
            
            original_images = images[:]
            category_total = len(original_images)
            downloaded_this_category = 0
            current_images = images[:]
            attempt = 1

            while attempt <= MAX_DOWNLOAD_ATTEMPTS and current_images:
                # Vérifier le temps écoulé
                if time.time() - start_time > MAX_PROCESSING_TIME:
                    print(f"[TIMEOUT] Processing timeout for product {product_id}")
                    update_product_status(product_id, "failed", conn)
                    return
                
                dl_count, success_images, err_retry, _ = download_images(current_images, product_id, category)
                if success_images:
                    insert_images(product_id, category, success_images, conn)
                
                downloaded_this_category += dl_count
                current_images = [current_images[i] for i in err_retry]
                attempt += 1

            product_total_images += category_total
            product_total_downloaded += downloaded_this_category

        success_ratio = product_total_downloaded / product_total_images if product_total_images > 0 else 0
        update_product_status(product_id, "downloaded" if success_ratio >= SUCCESS_THRESHOLD else "completed", conn)

    except Exception as e:
        print(f"[ERROR] Processing failed for product {product_id}: {e}")
        update_product_status(product_id, "failed", conn)


def process_products_in_batches(json_file: str):
    conn = psycopg2.connect(**DB_CONFIG)
    with open(json_file, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print("[ERROR] Invalid JSON structure.")
            return
    
    if isinstance(data, list): 
        products = data
    elif isinstance(data, dict):
        products = [data] if "productId" in data else [value for value in data.values() if isinstance(value, dict)]
    else:
        print("[ERROR] JSON data must be a list or dictionary with a 'productId'.")
        return 

    if not products:
        print("[INFO] No valid product data found in JSON file.")
        return

    while products:
        current_batch = products[:MAX_PRODUCTS_PER_ITER]
        products = products[MAX_PRODUCTS_PER_ITER:]

        threads = []
        for product in current_batch:
            thread = threading.Thread(target=process_product, args=(product, conn))
            threads.append(thread)
            thread.start()
            if len(threads) >= MAX_THREADS:
                for t in threads:
                    t.join()
                threads = []

        for t in threads:
            t.join()

    print("[INFO] All products processed")
    conn.close()

if __name__ == "__main__":
    start_time = time.time()
    print(f"[INFO] Start processing at {datetime.now()}")  

    json_file = fileName
    process_products_in_batches(json_file)
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"[INFO] Finished processing at {datetime.now()}")
    print(f"[INFO] Total time taken: {elapsed_time:.2f} seconds")
  