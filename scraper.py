import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import os
import time
from tqdm import tqdm
import string


# Function to safely fetch HTML content
def safe_read_html(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return BeautifulSoup(response.content, 'html.parser')
        else:
            print(f"Failed to fetch {url}, status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


# PART 1: Scraping Job Offer IDs from multiple pages
base_page_url = "https://www.pracuj.pl/praca/devops%20engineer;kw?"
N_pages = 3  # Number of pages to scrape
all_links = []

# Loop over specified pages to collect job offer links
for i in tqdm(range(1, N_pages + 1), desc="Scraping pages"):
    page_url = f"{base_page_url}{i}"
    page = safe_read_html(page_url)

    if page is not None:
        a_tags = page.find_all("a", class_="tiles_cnb3rfy core_n194fgoq")
        for a_tag in a_tags:
            href = a_tag.get("href")
            if href and href.startswith("https://www.pracuj.pl/praca/"):
                offer_id = re.findall(r'oferta,(\d+)', href)
                if offer_id:
                    all_links.append(offer_id[0])
        print(f"Page {i} processed successfully.")
    else:
        print(f"Failed to fetch page {i}.")

# Remove duplicates and format URLs for the final output
all_links = list(set(all_links))
all_links = [f"https://www.pracuj.pl/praca/x,oferta,{link}" for link in all_links]

# Save to CSV
df = pd.DataFrame(all_links, columns=['offer_url'])
df.to_csv('offer_urls.csv', index=False)
print(f"Total unique links collected: {len(all_links)}")


# PART 2: Scraping Job Offer Details
# Function to extract job offer details from each offer page
def get_offer_details(offer_url):
    offer_page = safe_read_html(offer_url)

    if offer_page:
        offer_title = offer_page.title.string.strip() if offer_page.title else ""
        requirements = "Requirements not found"

        # Search for requirements headers in multiple languages
        requirements_headers = ["Nasze wymagania", "Our requirements"]
        requirements_section = next(
            (offer_page.find("h2", string=header) for header in requirements_headers if
             offer_page.find("h2", string=header)),
            None
        )

        if requirements_section:
            requirements_div = requirements_section.find_next("div", class_="c1s1xseq")
            if requirements_div:
                requirements = " ".join(li.get_text(strip=True) for li in requirements_div.find_all("li"))

        return {
            "title": offer_title,
            "requirements": requirements,
            "url": offer_url
        }
    else:
        return {}


# Read offer URLs from CSV file
all_links = pd.read_csv("offer_urls.csv")['offer_url'].tolist()

# Create a folder to store scraped data if it doesn't exist
data_folder = "data"
os.makedirs(data_folder, exist_ok=True)

# Check for any existing data files to continue scraping if needed
existing_files = sorted([f for f in os.listdir(data_folder) if f.startswith("grabbed_offers")], reverse=True)
if not existing_files:
    offers = pd.DataFrame()
    start_i = 1
else:
    offers = pd.read_csv(os.path.join(data_folder, existing_files[0]))
    start_i = offers['i'].max() + 1

# Initialize counts for mentions of AWS, Azure, and both
aws_count = 0
azure_count = 0
both_count = 0
all_requirements = []

for i in tqdm(range(start_i, len(all_links) + 1), desc="Grabbing offers"):
    offer_url = all_links[i - 1]
    temp_offer = get_offer_details(offer_url)
    temp_offer['i'] = i

    if temp_offer:
        offers = pd.concat([offers, pd.DataFrame([temp_offer])], ignore_index=True)

        # Display requirements in console
        print(f"\nRequirements for Offer {i} ({temp_offer['title']}):")
        print(temp_offer["requirements"] or "No requirements specified.")

        if temp_offer["requirements"] != "Requirements not found":
            all_requirements.append(temp_offer["requirements"])
            requirements_text = temp_offer["requirements"].lower()

            # Check if "AWS" or "Amazon" is mentioned for cloud services
            aws_present = 'aws' in requirements_text or 'amazon' in requirements_text
            azure_present = 'azure' in requirements_text

            # Increment counts based on presence
            if aws_present:
                aws_count += 1  # Count AWS only once if present
            if azure_present:
                azure_count += 1  # Count Azure only once if present
            if aws_present and azure_present:
                both_count += 1  # Count both if both are present

    # Save progress every 25 offers
    if i % 25 == 0:
        offers.to_csv(f"data/grabbed_offers_{time.strftime('%Y_%m_%d_%H%M')}.csv", index=False)
        print("\nProgress saved!\n")

    time.sleep(0.4)

    # Save progress every 25 offers
    if i % 25 == 0:
        offers.to_csv(f"data/grabbed_offers_{time.strftime('%Y_%m_%d_%H%M')}.csv", index=False)
        print("\nProgress saved!\n")

    time.sleep(0.4)

# Final save of all collected data
offers.to_csv(f"data/grabbed_offers_{time.strftime('%Y_%m_%d_%H%M')}.csv", index=False)
# Data saved for further processing if necessary

# Summary of AWS and Azure mentions
print("\nSummary:")
print(f"AWS or Amazon mentioned in requirements: {aws_count} times")
print(f"Azure mentioned in requirements: {azure_count} times")
print(f"Both AWS (or Amazon) and Azure mentioned together: {both_count} times")
