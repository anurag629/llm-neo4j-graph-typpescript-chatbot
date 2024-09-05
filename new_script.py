import csv
import json
import logging
import threading
from queue import Queue
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    TimeoutException,
    NoSuchElementException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initialize the WebDriver options (assuming Chrome)
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--headless")  # Optional: Run in headless mode

# Database connection details
db_config = {
    "user": "root",
    "pw": quote_plus("@nuraG1!"),
    "db": "cake",
    "host": "localhost",
    "port": 3306,
}

# Create database engine
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['pw']}@{db_config['host']}/{db_config['db']}"
)
Session = sessionmaker(bind=engine)

# Configuration variables
PRICE_INCREMENT = 20  # Fixed price increment in euros
PRICE_PERCENTAGE = 0.0  # Percentage increase in price
PAGE_START = 1  # Start page for scraping
PAGE_END = 2  # End page for scraping

# Queue for storing products temporarily
product_queue = Queue()


def read_links_from_csv(file_path):
    """Read links from a CSV file."""
    with open(file_path, newline="") as csvfile:
        reader = csv.reader(csvfile)
        return [row[0] for row in reader]


def fetch_product_details(driver, thread_name):
    """Fetch product details from the collection page."""
    products = []
    try:
        # Example actions to close ads or pop-ups and change settings
        actions = [
            ('//*[@id="bx-close-inside-1848842"]', "Remove ADS."),
            ("/html/body/div[1]/div[2]/button", "Change Country."),
            ("/html/body/div[1]/div[2]/form/div/select", "Click."),
            (
                "/html/body/div[1]/div[2]/form/div/select/option[226]",
                "Select Country.",
            ),
            ("/html/body/div[1]/div[2]/form/button", "Submit Change."),
        ]
        for xpath, action_msg in actions:
            try:
                button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                logging.info(f"{thread_name} - {action_msg}")
                button.click()
                time.sleep(1)
            except (TimeoutException, NoSuchElementException) as e:
                logging.warning(
                    f"{thread_name} - Could not perform action: {action_msg}."
                )

        # Scroll through the page
        for scroll in range(1000, 5000, 1000):
            logging.info(f"{thread_name} - Scrolling by {scroll} pixels.")
            driver.execute_script(f"window.scrollBy(0, {scroll})")
            time.sleep(1)

        # Find and parse product data
        script_tags = driver.find_elements(
            By.XPATH, '//script[@type="application/ld+json" and @data-product-info]'
        )
        logging.info(f"{thread_name} - Found {len(script_tags)} product script tags.")
        for script_tag in script_tags:
            product_json = json.loads(script_tag.get_attribute("innerHTML"))
            products.append(product_json)
    except Exception as e:
        logging.error(
            f"{thread_name} - An error occurred while fetching product details: {e}"
        )
    return products


def handle_pagination(driver, page_start, page_end, thread_name):
    """Handle pagination of the product list."""
    current_page = page_start
    while True:
        logging.info(f"{thread_name} - Scraping page {current_page}")
        products = fetch_product_details(driver, thread_name)
        logging.info(
            f"{thread_name} - Fetched {len(products)} products from page {current_page}."
        )
        product_queue.put(products)  # Add scraped products to the queue
        try:
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//a[@class="pagination__btn--next"]')
                )
            )
            logging.info(
                f"{thread_name} - Clicking next page button for page {current_page}."
            )
            if current_page < page_end:
                try:
                    next_button.click()
                except ElementClickInterceptedException:
                    driver.execute_script("arguments[0].click();", next_button)
                time.sleep(1)
                current_page += 1
            else:
                logging.info(
                    f"{thread_name} - Reached the end of the specified page range."
                )
                break
        except (TimeoutException, NoSuchElementException) as e:
            logging.info(
                f"{thread_name} - No more pages to load or reached end of specified range."
            )
            break


def upsert_product(product_data, thread_name):
    """Insert or update product data in the database."""
    session = Session()
    try:
        existing_product = session.execute(
            text("SELECT 1 FROM products WHERE id = :id"), {"id": product_data["id"]}
        ).fetchone()
        if existing_product:
            logging.info(
                f"{thread_name} - Product with ID {product_data['id']} already exists. Skipping."
            )
            return
        query = text(
            """
            INSERT INTO products (id, id_product, product_name, slug, price, category)
            VALUES (:id, :id_product, :product_name, :slug, :price, :category)
            ON DUPLICATE KEY UPDATE
                product_name = VALUES(product_name),
                slug = VALUES(slug),
                price = VALUES(price),
                category = VALUES(category)
        """
        )
        session.execute(query, product_data)
        session.commit()
        logging.info(f"{thread_name} - Upserted product with ID {product_data['id']}.")
    except Exception as e:
        logging.error(
            f"{thread_name} - Failed to upsert product {product_data.get('id')}: {e}"
        )
    finally:
        session.close()


def insert_related_data_color_product(data, table_name, thread_name):
    """Insert data into related tables only if not already present."""
    session = Session()
    try:
        for record in data:
            existing_entry = session.execute(
                text(
                    f"SELECT 1 FROM {table_name} WHERE id_product = :id_product AND color_name = :color_name"
                ),
                {
                    "id_product": record["id_product"],
                    "color_name": record["color_name"],
                },
            ).fetchone()
            if existing_entry:
                logging.info(
                    f"{thread_name} - Entry for id_product {record['id_product']} already exists in {table_name}. Skipping."
                )
                continue
            query = text(
                f"INSERT INTO {table_name} ({', '.join(['`' + k + '`' for k in record.keys()])}) VALUES ({', '.join(':' + k for k in record.keys())})"
            )
            session.execute(query, record)
            session.commit()
            logging.info(
                f"{thread_name} - Inserted color data for id_product {record['id_product']} into {table_name}."
            )
    except Exception as e:
        logging.error(f"{thread_name} - Failed to insert data into {table_name}: {e}")
    finally:
        session.close()


def insert_related_data_image_product(data, table_name, thread_name):
    """Insert data into related tables only if not already present."""
    session = Session()
    try:
        for record in data:
            existing_entry = session.execute(
                text(
                    f"SELECT 1 FROM {table_name} WHERE id_product = :id_product AND img = :img"
                ),
                {"id_product": record["id_product"], "img": record["img"]},
            ).fetchone()
            if existing_entry:
                logging.info(
                    f"{thread_name} - Entry for id_product {record['id_product']} already exists in {table_name}. Skipping."
                )
                continue
            query = text(
                f"INSERT INTO {table_name} ({', '.join(['`' + k + '`' for k in record.keys()])}) VALUES ({', '.join(':' + k for k in record.keys())})"
            )
            session.execute(query, record)
            session.commit()
            logging.info(
                f"{thread_name} - Inserted image data for id_product {record['id_product']} into {table_name}."
            )
    except Exception as e:
        logging.error(f"{thread_name} - Failed to insert data into {table_name}: {e}")
    finally:
        session.close()


def insert_related_data_size_product(data, table_name, thread_name):
    """Insert data into related tables only if not already present."""
    session = Session()
    try:
        for record in data:
            existing_entry = session.execute(
                text(
                    f"SELECT 1 FROM {table_name} WHERE id_product = :id_product AND size = :size"
                ),
                {"id_product": record["id_product"], "size": record["size"]},
            ).fetchone()
            if existing_entry:
                logging.info(
                    f"{thread_name} - Entry for id_product {record['id_product']} already exists in {table_name}. Skipping."
                )
                continue
            query = text(
                f"INSERT INTO {table_name} ({', '.join(['`' + k + '`' for k in record.keys()])}) VALUES ({', '.join(':' + k for k in record.keys())})"
            )
            session.execute(query, record)
            session.commit()
            logging.info(
                f"{thread_name} - Inserted size data for id_product {record['id_product']} into {table_name}."
            )
    except Exception as e:
        logging.error(f"{thread_name} - Failed to insert data into {table_name}: {e}")
    finally:
        session.close()


def process_products(thread_name):
    """Process and store products and their related data."""
    with ThreadPoolExecutor() as executor:
        while True:
            all_products = product_queue.get()
            if all_products is None:  # Exit condition for the thread
                break
            logging.info(f"{thread_name} - Processing {len(all_products)} products.")
            for product in all_products:
                # print product in json format
                # logging.info(f"{thread_name} - {json.dumps(product, indent=4)}")
                product_data = {
                    "id": product.get("id"),
                    "id_product": product.get(
                        "id"
                    ),  # Assuming 'id' and 'id_product' are identical
                    "product_name": product.get("title"),
                    "slug": product.get("handle") or "dummy-slug",
                    "price": str(
                        int(
                            float(product.get("price")) * (1 + PRICE_PERCENTAGE)
                            + PRICE_INCREMENT
                        )
                    ),
                    "category": product.get("category"),
                }
                executor.submit(upsert_product, product_data, thread_name)
                variants_data = [
                    {
                        "id_product": product.get("id"),
                        "size": variant.get("title"),
                        "status": variant.get("inventory_quantity"),
                    }
                    for variant in product.get("variants", [])
                ]
                images_data = [
                    {"id_product": product.get("id"), "img": img.get("src")}
                    for img in product.get("media", [])
                ]

                colors_data = [
                    {
                        "id_product": product.get("id"),
                        "color_name": color_info[3],
                        "color": color_info[1],
                        "slugc": color_info[0],
                    }
                    for color in product.get("swatches", [])
                    if (color_info := color.split(":")) and len(color_info) == 4
                ]

                if variants_data:
                    executor.submit(
                        insert_related_data_size_product,
                        variants_data,
                        "products_size",
                        thread_name,
                    )
                if images_data:
                    executor.submit(
                        insert_related_data_image_product,
                        images_data,
                        "products_images",
                        thread_name,
                    )
                if colors_data:
                    executor.submit(
                        insert_related_data_color_product,
                        colors_data,
                        "products_color",
                        thread_name,
                    )


def scrape_collection(collection_url, thread_name):
    """Scrape a single collection URL."""
    driver = webdriver.Chrome(options=chrome_options)
    try:
        logging.info(f"{thread_name} - Opening collection page: {collection_url}")
        driver.get(collection_url)
        time.sleep(5)  # Wait for the page to load for 5 seconds

        # Start the insertion process in a separate thread
        insertion_thread = threading.Thread(
            target=process_products, args=(thread_name,)
        )
        insertion_thread.start()

        handle_pagination(driver, PAGE_START, PAGE_END, thread_name)

        # Signal the insertion thread to exit
        product_queue.put(None)
        insertion_thread.join()
    except Exception as e:
        logging.error(
            f"{thread_name} - An error occurred while scraping {collection_url}: {e}"
        )
    finally:
        driver.quit()


# Main script execution
try:
    # Read collection URLs from a CSV file
    collection_links = read_links_from_csv("collection_links.csv")

    # Use a ThreadPoolExecutor to handle multiple collections concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:
        for i, link in enumerate(collection_links):
            thread_name = f"Thread-{i+1}"
            executor.submit(scrape_collection, link, thread_name)

except Exception as e:
    logging.error(f"An error occurred: {e}")
