import requests
import csv
import json
import time
from threading import Lock, Thread
from queue import Queue
from pathlib import Path

# Constants
API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"
BATCH_SIZE = 1000
MAX_RETRIES = 3
CHECKPOINT_FILE = "checkpoint.json"
ERROR_LOG_FILE = "error_log.json"
OUTPUT_DIR = Path("output")

# Headers để giả lập trình duyệt
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "application/json"
}

OUTPUT_DIR.mkdir(exist_ok=True)

# Load product IDs from CSV
def load_product_ids(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        return [row['id'] for row in reader]

product_ids = load_product_ids('product_ids.csv')

# Load checkpoint
try:
    with open(CHECKPOINT_FILE, 'r') as file:
        processed_ids = set(json.load(file))
except FileNotFoundError:
    processed_ids = set()

# Error logging
def log_error(product_id, error_message):
    with open(ERROR_LOG_FILE, 'a') as file:
        file.write(json.dumps({"product_id": product_id, "error": error_message}) + '\n')
    print(f"[ERROR] Product ID: {product_id} - {error_message}")  # Thông báo lỗi ra màn hình

# Lock để đồng bộ hóa
lock = Lock()

# Fetch product details
def fetch_product(product_id):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(API_URL.format(product_id), headers=HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Normalize description
            description = data.get("description", "").replace("\n", " ").strip()

            print(f"[SUCCESS] Fetched Product ID: {product_id}")  # Thông báo sản phẩm lấy thành công

            return {
                "id": data.get("id"),
                "name": data.get("name"),
                "url_key": data.get("url_key"),
                "price": data.get("price"),
                "description": description,
                "images_url": [img.get("base_url") for img in data.get("images", [])]
            }
        except Exception as e:
            retries += 1
            time.sleep(2)
            if retries == MAX_RETRIES:
                log_error(product_id, str(e))
                return None

# Save checkpoint
def save_checkpoint(processed_ids):
    with open(CHECKPOINT_FILE, 'w') as file:
        json.dump(list(processed_ids), file)
    print(f"[CHECKPOINT] Saved checkpoint with {len(processed_ids)} processed IDs")  # Thông báo checkpoint

# Queue for task distribution
product_queue = Queue()
for pid in product_ids:
    if pid not in processed_ids:
        product_queue.put(pid)

# Worker function
def worker():
    results = []
    while not product_queue.empty():
        pid = product_queue.get()
        print(f"[PROCESSING] Product ID: {pid}")  # Thông báo sản phẩm đang xử lý
        result = fetch_product(pid)
        if result:
            with lock:
                processed_ids.add(pid)
                results.append(result)

        # Save batch
        if len(results) >= BATCH_SIZE:
            batch_file = OUTPUT_DIR / f"products_{len(processed_ids)}.json"
            with open(batch_file, 'w') as file:
                json.dump(results, file, ensure_ascii=False, indent=2)
            results = []
            save_checkpoint(processed_ids)

        product_queue.task_done()

    # Save remaining results
    if results:
        batch_file = OUTPUT_DIR / f"products_{len(processed_ids)}.json"
        with open(batch_file, 'w') as file:
            json.dump(results, file, ensure_ascii=False, indent=2)
        save_checkpoint(processed_ids)

# Main execution with timing
start_crawl_time = time.time()

threads = []
for _ in range(20):  # Giữ lại 20 luồng để xử lý
    t = Thread(target=worker)
    t.start()
    threads.append(t)

for t in threads:
    t.join()

end_crawl_time = time.time()
print(f"[COMPLETED] Total time taken: {end_crawl_time - start_crawl_time:.2f} seconds")

print("[DONE] Data fetching completed.")
