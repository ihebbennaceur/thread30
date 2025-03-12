import sys, os, json, time, threading, io
import pycurl, certifi
import unicodedata, re
import random
from datetime import datetime, timedelta, timezone

import signal
import psycopg2
from typing import List, Dict, Tuple
import shutil  # Import shutil module

# Directory containing JSON files
JSON_DIR = "./todo_jsons"
# Directory to move processed JSON files
DONE_JSON_DIR = "./finished_jsons"

# Configuration
MAX_DOWNLOAD_ATTEMPTS = 2
IMAGE_EXTENSIONS_REGEX = re.compile(r"(?i)(\.jpg|\.jpeg|\.png|\.gif)")

MAX_PRODUCTS_PER_ITER = 2
SUCCESS_THRESHOLD = 0.9  
MAX_THREADS = 2  # Nombre maximum de threads pour le téléchargement d'images

DB_CONFIG = {
    "dbname": "newdb",
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

def create_image_path(img_url: str, product_id: str, index: int, category: str, base_dir: str) -> str:
    match = IMAGE_EXTENSIONS_REGEX.search(img_url)
    ext = match.group(0) if match else ".jpg"
    filename = f"{product_id}_{index}{ext}"
    return os.path.join(base_dir, product_id, category, filename)

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

 #new for sku
def get_downloaded_image_urls(product_id: str, conn) -> set:
    """Récupère les URLs des images déjà téléchargées pour un produit donné."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT original_url FROM mimages WHERE product_id = %s", (product_id,)
            )
            return {row[0] for row in cur.fetchall()}
    except Exception as e:
        print(f"[ERROR] Impossible de récupérer les images déjà téléchargées pour {product_id}: {e}")
        return set()

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

#v2 sku
# Dictionnaire global pour stocker les URLs d'images et leurs chemins locaux
image_url_to_local_path = {}

def download_images(image_urls: List[str], product_id: str, category: str, base_dir: str) -> Tuple[int, List[Tuple[str, str]], List[int], List]:
    image_paths = []
    success_count = 0
    success_images = []
    aiimg_err_retry = []
    hiimg_err_final = []  

    product_folder = os.path.join(base_dir, product_id, category)
    os.makedirs(product_folder, exist_ok=True)

    def download_thread(img_url, file_path, idx):
        nonlocal success_count
        # Vérifier si l'URL a déjà été téléchargée
        if (img_url in image_url_to_local_path):
            success_count += 1
            success_images.append((img_url, image_url_to_local_path[img_url]))
            print(f"[INFO] Image {img_url} already downloaded, using cached path: {image_url_to_local_path[img_url]}")
        else:
            if download_image(img_url, file_path):
                success_count += 1
                success_images.append((img_url, file_path))
                image_url_to_local_path[img_url] = file_path  # Ajouter au dictionnaire
                print(f"[SUCCESS] {img_url} -> {file_path}")
            else:
                aiimg_err_retry.append(idx)

    threads = []
    for idx, img_url in enumerate(image_urls):
        file_path = create_image_path(img_url, product_id, idx + 1, category, base_dir)
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

# Dictionnaire global pour stocker les URLs d'images et leurs chemins locaux
image_url_to_local_path = {}

def clear_image_cache():
    """Vide le dictionnaire global pour libérer la mémoire."""
    global image_url_to_local_path
    image_url_to_local_path.clear()
    print("[INFO] Image cache cleared.")

def process_product(product, conn, base_dir: str):
    product_id = product.get("productId")
    if not product_id:
        print(f"[ERROR] Missing 'productId' in product: {product}")
        return

    update_product_status(product_id, "processing", conn)
    start_time = time.time()  # Début du traitement
    MAX_PROCESSING_TIME = 100  # 600 = 10 minutes en secondes

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
                
                dl_count, success_images, err_retry, _ = download_images(current_images, product_id, category, base_dir)
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
    finally:
        # Vider le cache des images après le traitement du produit
        clear_image_cache()
        
BASE_DIR = os.path.join(os.getcwd(), f"/home/ta/Desktop/disk19/IMG/")

def process_products_in_batches(json_file: str):
    conn = psycopg2.connect(**DB_CONFIG)
    base_name = os.path.splitext(os.path.basename(json_file))[0]
    base_dir = os.path.join(BASE_DIR, base_name)
    os.makedirs(base_dir, exist_ok=True)
    
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
            thread = threading.Thread(target=process_product, args=(product, conn, base_dir))
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

    # Move the processed JSON file to the done_jsons directory
    shutil.move(json_file, os.path.join(DONE_JSON_DIR, os.path.basename(json_file)))
    print(f"[INFO] Moved {json_file} to {DONE_JSON_DIR}")

if __name__ == "__main__":
    start_time = time.time()
    print(f"[INFO] Start processing at {datetime.now()}")  
    
    # Create the done_jsons directory if it doesn't exist
    os.makedirs(DONE_JSON_DIR, exist_ok=True)

    # Process all JSON files in the specified directory
    for json_file in os.listdir(JSON_DIR):
        if json_file.endswith(".json"):
            process_products_in_batches(os.path.join(JSON_DIR, json_file))
    
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"[INFO] Finished processing at {datetime.now()}")
    print(f"[INFO] Total time taken: {elapsed_time:.2f} seconds")
  
    close_db(conn)