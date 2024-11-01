import csv
import ftplib
import requests  # type: ignore
import openai  # type: ignore
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize the OpenAI client with your API key
openai.api_key = os.getenv('OPENAI_API_KEY')

# Logging settings
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# FTP connection details
FTP_HOST = '34.173.155.236'
FTP_PORT = 21
FTP_USER = os.getenv('FTP_USER')
FTP_PASS = os.getenv('FTP_PASS')
FTP_FILE_PATH = '/inventory.txt'
LOCAL_FILE_PATH = '/tmp/inventory.txt'

# Adalo API endpoints
ADALO_API_KEY = os.getenv('ADALO_API_KEY')
ADALO_API_URL = 'https://api.adalo.com/v0/apps/a22a9592-393a-43dc-96ad-f6cb0711e757/collections/t_5bdw65x8absxj7cz54mk4eut3'
ADALO_IMG_API_URL = 'https://api.adalo.com/v0/apps/a22a9592-393a-43dc-96ad-f6cb0711e757/collections/t_b24dxcr061y05gfaqi0tz1w6p'

# Function to format text from the OptionText column
def format_option_text(text):
    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a text formatting assistant. Your task is to format unstructured lists of car features "
                    "into a structured format with sections and bullet points, as shown in the following template. "
                    "Each feature should be grouped under its respective section heading, with proper indentation and "
                    "organization. Maintain the same structure and style as the example below:\n\n"
                    "**Features**\n\n"
                    "**Air Conditioning**\n"
                    "- Air Filtration\n"
                    "- Front Air Conditioning\n"
                    "- Front Air Conditioning Zones: Single\n"
                    "- Rear Vents: Second Row\n\n"
                    "**Airbags**\n"
                    "- Airbag Deactivation: Occupant Sensing Passenger\n"
                    "- Front Airbags: Dual\n"
                    "- Knee Airbags: Driver\n"
                    "- Side Airbags: Front\n"
                    "- Side Curtain Airbags: Front\n"
                    "- Side Curtain Airbags: Rear\n\n"
                    "**Audio System**\n"
                    "- Antenna Type: Mast\n"
                    "- Auxiliary Audio Input: Bluetooth\n"
                    "- Auxiliary Audio Input: iPod/iPhone\n"
                    "- Auxiliary Audio Input: USB\n"
                    "- Digital Sound Processing\n"
                    "- In-Dash CD: MP3 Playback\n"
                    "- In-Dash CD: Single Disc\n"
                    "- Radio: AM/FM\n"
                    "- Speed Sensitive Volume Control\n"
                    "- Total Speakers: 4\n"
                    "- Watts: 140\n\n"
                    "... (continue for all features)\n\n"
                    "Instructions for Formatting:\n\n"
                    "• Start with a section title (e.g., “Air Conditioning”).\n"
                    "• List each feature under the appropriate section.\n"
                    "• Use a hyphen “-” to indicate a list item.\n"
                    "• Group similar features together under the same section.\n"
                    "• Ensure consistency in formatting (e.g., title capitalization, spacing)."
                )
            },
            {
                "role": "user",
                "content": text
            }
        ],
    )
    formatted_text = response['choices'][0]['message']['content']
    return formatted_text.strip()

# Function to format text from the Description column
def format_description_text(text):
    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a text formatting assistant. Your task is to format the Description: delete HTML or "
                    "other tags and format it into human-friendly text."
                )
            },
            {
                "role": "user",
                "content": text
            }
        ],
    )
    formatted_text = response['choices'][0]['message']['content']
    return formatted_text.strip()

# Function to download a file from FTP
def download_file_from_ftp():
    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        
        logging.info("Current directory: %s", ftp.pwd())
        
        files = ftp.nlst()
        logging.info("Files on FTP server: %s", files)
        
        if FTP_FILE_PATH.strip('/') in files:
            with open(LOCAL_FILE_PATH, 'wb') as local_file:
                ftp.retrbinary(f'RETR {FTP_FILE_PATH}', local_file.write)
            logging.info("File %s downloaded successfully.", FTP_FILE_PATH)
        else:
            logging.error("File %s not found on FTP server", FTP_FILE_PATH)

# Function to read a CSV file
def read_inventory_file():
    with open(LOCAL_FILE_PATH, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        inventory_data = [row for row in csv_reader]
    return inventory_data

# Function to get all records from Adalo
def get_adalo_records(api_url):
    headers = {
        'Authorization': f'Bearer {ADALO_API_KEY}',
        'Content-Type': 'application/json'
    }
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get('records', [])

# Function to add a new record in Adalo
def add_record_to_adalo(api_url, record):
    headers = {
        'Authorization': f'Bearer {ADALO_API_KEY}',
        'Content-Type': 'application/json'
    }
    response = requests.post(api_url, json=record, headers=headers)
    response.raise_for_status()

# Function to delete a record from Adalo
def delete_record_from_adalo(api_url, record_id):
    headers = {
        'Authorization': f'Bearer {ADALO_API_KEY}',
        'Content-Type': 'application/json'
    }
    response = requests.delete(f'{api_url}/{record_id}', headers=headers)
    response.raise_for_status()

# Function for processing images and adding them to the image table
def handle_images(vin, img_urls):
    img_list = img_urls.split(',')
    main_img_url = img_list[0].strip() if img_list else ''  # First URL for Main IMG
    
    # Add all images to the image table
    for img_url in img_list:
        img_url = img_url.strip()
        if img_url:
            add_record_to_adalo(ADALO_IMG_API_URL, {'VIN': vin, 'IMG URL': img_url})
    
    return main_img_url

# The main function for data synchronization
def sync_data():
    logging.info("Starting data synchronization process.")
    
    try:
        download_file_from_ftp()
    except Exception as e:
        logging.error(f"Error during FTP download: {e}")
        return

    try:
        inventory_data = read_inventory_file()
    except Exception as e:
        logging.error(f"Error reading inventory file: {e}")
        return

    logging.info("Successfully read inventory file with %d records.", len(inventory_data))

    try:
        adalo_data = get_adalo_records(ADALO_API_URL)
    except Exception as e:
        logging.error(f"Error retrieving Adalo records: {e}")
        return

    adalo_vins = {record['VIN']: record['id'] for record in adalo_data}
    logging.info("Successfully retrieved %d records from Adalo database.", len(adalo_vins))

    total_changes = 0

    # Add new records
    for index, item in enumerate(inventory_data):
        try:
            vin = item['VIN']
            if vin not in adalo_vins:
                logging.info("Adding new record for VIN %s (%d/%d)", vin, index + 1, len(inventory_data))
                
                img_urls = item.get('images', '')  # Check for the presence of the 'images' key
                main_img_url = handle_images(vin, img_urls)
                item['Main IMG'] = main_img_url

                # Text formatting for OptionText and Description columns
                item['OptionText'] = format_option_text(item.get('OptionText', ''))
                item['Description'] = format_description_text(item.get('Description', ''))
                
                add_record_to_adalo(ADALO_API_URL, item)
                total_changes += 1
            else:
                logging.info("VIN %s already exists. Skipping.", vin)
        except Exception as e:
            logging.error(f"Error processing VIN {vin}: {e}")

    # Delete records that are not in the inventory
    inventory_vins = {item['VIN'] for item in inventory_data}
    for vin, record_id in adalo_vins.items():
        try:
            if vin not in inventory_vins:
                logging.info("Deleting record for VIN %s", vin)
                delete_record_from_adalo(ADALO_API_URL, record_id)
                total_changes += 1
        except Exception as e:
            logging.error(f"Error deleting VIN {vin}: {e}")

    logging.info("Total changes made: %d", total_changes)

if __name__ == '__main__':
    logging.info("Application started.")
    sync_data()
    logging.info("Application finished.")
