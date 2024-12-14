from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import zipfile
import shutil
import glob

# Set up the Selenium WebDriver
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Run in headless mode if you don't want a browser window popping up  # Run in headless mode if you don't want a browser window popping up

# Set up custom download directory
download_dir = os.path.join(os.getcwd(), "downloads")
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

prefs = {"download.default_directory": download_dir}
options.add_experimental_option("prefs", prefs)

# Instantiate the driver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Define schedule URLs with their reference codes
SCHEDULE_URLS = {
    'B1': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FKM&QO_fu146_anzr=",  #Schedule B-1 (Balance Sheet)
    'B1_1': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FLF&QO_fu146_anzr=",  #Schedule B-1.1
    'B43': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=GEH&QO_fu146_anzr=",  #Schedule B-43 Inventory
    'P1A': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=GEF&QO_fu146_anzr=",  #Schedule P-1(a) Employees
    'P1_1': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FMD&QO_fu146_anzr=",  #Schedule P-1.1
    'P1_2': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FMI&QO_fu146_anzr=",  #Schedule P-1.2 Statement of Operations
    'P10': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=GDF&QO_fu146_anzr=",  #Schedule P-10
    'P12A': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FMH&QO_fu146_anzr=",  #Schedule P-12a
    'P5_1': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FMJ&QO_fu146_anzr=",  #Schedule P-5.1
    'P5_2': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FMK&QO_fu146_anzr=",  #Schedule P-5.2
    'P6': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FME&QO_fu146_anzr=",  #Schedule P-6 Operating Expenses by Objective Function
    'P7': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FKL&QO_fu146_anzr=",  #Schedule P-7 Operating Expenses by Functional Grouping
    'T1': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FJH&QO_fu146_anzr=",  #T1 Traffic and Capacity Summary
    'T2': "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FIH&QO_fu146_anzr=",  #T2 data Traffic, Capacity and Operations by Aircraft Type
}

# Update the loop to iterate through the dictionary
for code, url in SCHEDULE_URLS.items():
    # Get the schedule name from the comment
    schedule_name = next((comment.strip() for line in [f"{url} #{comment}" for code, url, comment in 
                         [(k, v, v.split('#')[1] if '#' in v else '') for k, v in SCHEDULE_URLS.items()]] 
                         if url in line for comment in line.split('#')[1:]), "Unknown Schedule")
    print(f"\nProcessing {code} - {schedule_name}...")
    
    # Open the target URL
    driver.get(url)

    # Give the page some time to load
    time.sleep(5)  # Added to observe actions

    # Find the dropdown filter for years and select "All Years"
    year_dropdown = Select(driver.find_element(By.ID, "cboYear"))
    year_dropdown.select_by_visible_text("All Years")

    # Give it a moment to process the selection
    time.sleep(2)  # Added to observe actions

    # Find and select the "Select all fields" checkbox
    select_all_fields_checkbox = driver.find_element(By.ID, "chkAllVars")
    if not select_all_fields_checkbox.is_selected():
        select_all_fields_checkbox.click()

    # Give it a moment to process the selection
    time.sleep(2)  # Added to observe actions

    # Find the download button and click it
    download_button = driver.find_element(By.ID, "btnDownload")
    download_button.click()

    # Wait for the download to complete by checking for the absence of .crdownload files
    download_complete = False
    while not download_complete:
        time.sleep(5)  # Added to observe actions
        download_complete = not any([filename.endswith(".crdownload") for filename in os.listdir(download_dir)])

# Close the browser
driver.quit()

# Extract the downloaded ZIP files to get the CSV files
for filename in os.listdir(download_dir):
    if filename.endswith(".zip"):
        print(f"Extracting {filename}...")
        zip_path = os.path.join(download_dir, filename)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(download_dir)
        # Optionally, delete the ZIP file after extraction
        os.remove(zip_path)

print("Download and extraction process completed.")
