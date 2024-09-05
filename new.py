import json
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import ElementClickInterceptedException
from urllib.parse import quote_plus
import time
import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initialize the WebDriver (assuming Chrome)
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--disable-gpu")
# chrome_options.add_argument("--headless")  # Run in headless mode
driver = webdriver.Chrome(options=chrome_options)

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
session = Session()

# Configuration variable
SCRAPE_SINGLE_COLLECTION = (
    True  # Set to True to scrape only one collection, False for all
)


# Function to fetch product details from the collection page
def fetch_product_details():
    products = []
    try:
        ads_button = driver.find_elements(
            By.XPATH, '//*[@id="bx-close-inside-1848842"]'
        )
        if ads_button:
            logging.info("Remove ADS.")
            ads_button[0].click()
            time.sleep(1)  # Wait for the next page to load
        else:
            logging.info("No ADS.")

        ads1_button = driver.find_elements(
            By.XPATH, "/html/body/div[1]/div[1]/div[2]/button"
        )
        if ads_button:
            logging.info("Change Country.")
            ads1_button[0].click()
            time.sleep(1)  # Wait for the next page to load
        else:
            logging.info("No ADS.")

        ads2_button = driver.find_elements(
            By.XPATH, "/html/body/div[1]/div[1]/div[2]/form/div/select"
        )
        if ads_button:
            logging.info("Click.")
            ads2_button[0].click()
            time.sleep(1)  # Wait for the next page to load
        else:
            logging.info("No ADS.")

        ads3_button = driver.find_elements(
            By.XPATH, "/html/body/div[1]/div[1]/div[2]/form/div/select/option[226]"
        )
        if ads_button:
            logging.info("Select Country.")
            ads3_button[0].click()
            time.sleep(1)  # Wait for the next page to load
        else:
            logging.info("No ADS.")

        ads4_button = driver.find_elements(
            By.XPATH, "/html/body/div[1]/div[1]/div[2]/form/button"
        )
        if ads_button:
            logging.info("Submit Change")
            ads4_button[0].click()
            time.sleep(1)  # Wait for the next page to load
        else:
            logging.info("No ADS.")

        # Scroll to the bottom of the page
        driver.execute_script("window.scrollBy(0, 1000)")
        time.sleep(1)
        driver.execute_script("window.scrollBy(0, 1500)")
        time.sleep(1)
        driver.execute_script("window.scrollBy(0, 2000)")
        time.sleep(1)
        driver.execute_script("window.scrollBy(0, 2500)")
        time.sleep(1)
        driver.execute_script("window.scrollBy(0, 3000)")
        time.sleep(1)
        driver.execute_script("window.scrollBy(0, 3500)")
        time.sleep(1)
        driver.execute_script("window.scrollBy(0, 4000)")
        time.sleep(1)
        driver.execute_script("window.scrollBy(0, 4500)")

        # Find all script tags with type 'application/ld+json' that have product information
        script_tags = driver.find_elements(
            By.XPATH, '//script[@type="application/ld+json" and @data-product-info]'
        )
        for script_tag in script_tags:
            product_json = json.loads(script_tag.get_attribute("innerHTML"))
            products.append(product_json)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    return products


# Function to handle pagination for a collection
def handle_pagination():
    while True:
        products = fetch_product_details()
        process_products(products)  # Process and insert products immediately
        try:
            # Check if there is a next page
            next_button = driver.find_elements(
                By.XPATH, '//a[@class="pagination__btn--next"]'
            )
            if next_button:
                logging.info("Next page button found. Clicking to load more products.")
                try:
                    next_button[0].click()
                except ElementClickInterceptedException:
                    driver.execute_script("arguments[0].click();", next_button[0])
                time.sleep(1)  # Wait for the next page to load
            else:
                logging.info("No more pages to load.")
                break  # No more pages
        except Exception as e:
            logging.error(f"An error occurred during pagination: {e}")
            break


# Insert or update product into the database
def upsert_product(product_data):
    # Log the product data
    if not product_data.get("slug"):
        product_data["slug"] = "dummy-slug"  # Assign a dummy value if slug is missing

    # Check if the product already exists in the database
    existing_product = session.execute(
        text("SELECT * FROM products WHERE id = :id"), {"id": product_data["id"]}
    ).fetchone()

    if existing_product is None:
        query = text(
            """
        INSERT INTO products (id, product_name, slug, price, category)
        VALUES (:id, :product_name, :slug, :price, :category)
        ON DUPLICATE KEY UPDATE
            product_name = VALUES(product_name),
            slug = VALUES(slug),
            price = VALUES(price),
            category = VALUES(category)
        """
        )
        session.execute(query, product_data)


# Insert data into related tables
def insert_related_data(data, table_name):
    if not data:
        return

    for entry in data:
        # Build a condition for existing records
        conditions = " AND ".join([f"{key} = :{key}" for key in entry.keys()])

        # Check if the entry already exists in the table
        existing_entry = session.execute(
            text(f"SELECT * FROM {table_name} WHERE {conditions}"), entry
        ).fetchone()

        if existing_entry is None:
            query = text(
                f"""
                INSERT INTO {table_name} ({', '.join(['`' + k + '`' for k in entry.keys()])})
                VALUES ({', '.join(':' + k for k in entry.keys())})
            """
            )
            session.execute(query, entry)


# Extract and process product data
def process_products(all_products):
    product_data_list = []
    variants_data = []
    images_data = []
    colors_data = []

    for product in all_products:
        product_data = {
            "id": product.get("id"),
            "product_name": product.get("title"),
            "slug": product.get("handle")
            or "dummy-slug",  # Assign dummy value if slug is missing
            "price": product.get("price") + product.get("price"),
            "category": product.get("category"),
        }
        product_data_list.append(product_data)

    # Insert or update products
    for product_data in product_data_list:
        upsert_product(product_data)
    session.commit()  # Commit products

    # Process variants, images, and colors
    for product in all_products:
        # Process variants information
        for variant in product.get("variants", []):
            variant_data = {
                "id_product": product.get("id"),
                "size": variant.get("title"),  # Variant size
                "status": variant.get("inventory_quantity"),
            }
            variants_data.append(variant_data)

        # Process images information
        for img in product.get("media", []):
            img_data = {"id_product": product.get("id"), "img": img.get("src")}
            images_data.append(img_data)

        # Process color information
        if "swatches" in product:
            for color in product.get("swatches", []):
                # Parse the color information
                color_info = color.split(":")
                if len(color_info) == 4:
                    color_data = {
                        "id_product": product.get("id"),
                        "color_name": color_info[3],
                        "color": color_info[1],
                        "slugc": str(color_info[0]),
                    }
                    colors_data.append(color_data)
                    # Log the color data
                    # logging.info(f"Processed color data: {color_data}")

    # Insert data into related tables
    if variants_data:
        insert_related_data(variants_data, "products_size")
    if images_data:
        insert_related_data(images_data, "products_images")
    if colors_data:
        insert_related_data(colors_data, "products_color")

    session.commit()  # Commit all related data


# Main script to navigate through collections
try:
    if SCRAPE_SINGLE_COLLECTION:
        # Specify a single collection URL for testing
        test_collection_url = (
            "https://www.fashionnova.com/collections/jeans&itemsPerPage=120"
        )
        logging.info(f"Opening collection page: {test_collection_url}")
        driver.get(test_collection_url)
        time.sleep(5)  # Wait for the page to load

        # Handle pagination and scrape product details
        handle_pagination()
    else:
        # Scrape all collections (placeholder URL, adjust as needed)
        collections = ["https://www.fashionnova.com/collections/women?itemsPerPage=120"]
        for collection_url in collections:
            logging.info(f"Opening collection page: {collection_url}")
            driver.get(collection_url)
            time.sleep(5)  # Wait for the page to load

            # Handle pagination and scrape product details
            handle_pagination()

except Exception as e:
    logging.error(f"An error occurred: {e}")

# Close the browser
driver.quit()
