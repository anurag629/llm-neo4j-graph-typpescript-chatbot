import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

# Set up the Chrome driver
options = Options()
options.add_argument("--headless")  # Run in headless mode for efficiency
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()), options=options
)

# URL of the collections page
url = "https://www.fashionnova.com/collections"
driver.get(url)

# Give the page some time to load
time.sleep(3)

# Define the XPath for the collection links
links_xpath = "/html/body/div[4]/main/div/div[2]/div/div/a"

# Find all elements matching the XPath
elements = driver.find_elements(By.XPATH, links_xpath)

# Extract the href attribute from each element
links = [element.get_attribute("href") for element in elements]

# Save the links to a CSV file
csv_file_path = "collection_links.csv"
with open(csv_file_path, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["Links"])  # Header
    for link in links:
        writer.writerow([link])

# Close the driver
driver.quit()

print(f"Links saved to {csv_file_path}")
