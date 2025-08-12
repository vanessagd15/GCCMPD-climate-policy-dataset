import time
import os
import random
import re
from lxml import etree
import requests
from fake_useragent import UserAgent
import json
import csv

# Configuration: Only extract policies from this year onwards
MIN_YEAR = 2021

# Create output directory if it doesn't exist
output_dir = os.path.join(os.getcwd(), "data_new")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_file = os.path.join(output_dir, "ICAP_ETS.csv")


def get_page(url, max_retries=3):
    """Get page content with retry logic and better error handling"""
    for attempt in range(max_retries):
        try:
            ua = UserAgent()
            usr_ag = ua.random
            headers = {'User-Agent': usr_ag}
            
            # Increase timeout and add random delay
            response = requests.get(url, headers=headers, timeout=10)
            print(f"Status: {response.status_code}")
            response.encoding = 'utf-8'
            
            # Add random delay between requests (1-3 seconds)
            time.sleep(random.uniform(1, 3))
            
            return response.text
            
        except requests.exceptions.ReadTimeout:
            print(f"⏰ Timeout on attempt {attempt + 1}/{max_retries} for {url}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # Progressive backoff: 5, 10, 15 seconds
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed to fetch {url} after {max_retries} attempts")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 2)
            else:
                return None
    
    return None


print(f"🚀 Starting ICAP ETS crawler")
print(f"📂 Output file: {output_file}")
print(f"📅 Filtering for policies from {MIN_YEAR} onwards")
print(f"🏭 Source: International Carbon Action Partnership (ICAP)")
print("=" * 60)

# Keep track of progress
saved_count = 0
skipped_count = 0


# Fetch the list of ETS systems
print("🔍 Fetching list of ETS systems...")
url_1 = 'https://icapcarbonaction.com/en/json/maplist'

try:
    res_1 = get_page(url_1)
    if res_1 is None:
        print("❌ Could not fetch ETS list, exiting...")
        exit(1)
        
    js_data = json.loads(res_1)
    print(f"✅ Found {len(js_data)} ETS systems")
    
    # Extract system IDs
    id_ls = []
    for j_1 in js_data:
        if 'id' in j_1:
            id_ls.append(j_1['id'])
            
    print(f"📊 Processing {len(id_ls)} ETS systems")
    
except Exception as e:
    print(f"❌ Error fetching ETS list: {e}")
    exit(1)

# Check if file exists, if not create with headers
if not os.path.exists(output_file):
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as e:
        csv_writer = csv.writer(e)
        csv_writer.writerow(['Policy', 'Year', 'Country', 'Policy_Content', 'URL', 'Scope', 'Allocation', 'Sectoral coverage', 'GHGs covered', 'Offsets credits', 'Cap', 'Source'])
# Process each ETS system
for system_index, single_id in enumerate(id_ls):
    try:
        print(f'=========== 🏭 Processing ETS {system_index + 1}/{len(id_ls)} ===========')
        print(f'System ID: {single_id}')
        
        url_2 = f'https://icapcarbonaction.com/en/ets_system/{single_id}'
        
        res_2 = get_page(url_2)
        if res_2 is None:
            print(f"⚠️  Skipping ETS system {single_id} due to network error")
            continue
            
        data_2 = etree.HTML(res_2)
        
        # Extract Policy name
        try:
            Policy = data_2.xpath('//h1[@class="ets-caption"]/text()')[0].strip()
        except:
            Policy = ''
            
        if not Policy:
            print("⚠️  No ETS system name found, skipping...")
            continue
            
        print(f"🏭 ETS System: {Policy}")
        
        # Extract Year (start of operation)
        try:
            year_elements = data_2.xpath('//div[@class="field field--label-above field--type-integer field-start-operation-year"]/div[2]/text()')
            if year_elements:
                Year = year_elements[0].replace("\n", ' ').strip()
            else:
                Year = ''
        except:
            Year = ''
            
        print(f"📅 Start Year: {Year}")
        
        # Extract Country/Region
        try:
            country_elements = data_2.xpath('//div[@class="field field--label-above field--type-entity_reference field-regions"]/div[2]/div[@class="field__content"]/text()')
            if country_elements:
                Country = country_elements[0].replace("\n", ' ').strip()
            else:
                Country = ''
        except:
            Country = ''
        
        # Extract Policy Content (summary)
        try:
            Policy_Content_ls = data_2.xpath('//div[@class="field field--label-above field--type-text_long field-summary-short dropdown-menu hide-frame"]/div[@class="dropdown-menu__frame"]//text()')
            Policy_Content = ''
            for single_Policy in Policy_Content_ls:
                if single_Policy.strip():
                    Policy_Content += single_Policy.strip() + ' '
            Policy_Content = Policy_Content.strip()
        except:
            Policy_Content = ''
        
        # URL
        URL = url_2
        
        # Determine Scope based on policy name
        if '-' in Policy:
            Scope = 'SubNational'
            # Extract country from policy name if not already extracted
            if not Country:
                Country = Policy.split('-')[0].strip()
        else:
            Scope = 'National'
        
        print(f"🌍 Country: {Country}, 📊 Scope: {Scope}")
        
        # Extract Allocation method
        try:
            allocation_elements = data_2.xpath('//div[@class="field field--label-above field--type-string field-allowance-alloc-summary"]/div[2]/text()')
            if allocation_elements:
                Allocation = allocation_elements[0].replace("\n", ' ').strip()
            else:
                Allocation = ''
        except:
            Allocation = ''
        
        # Extract Sectoral coverage
        try:
            sectoral_elements = data_2.xpath('//div[@class="field field--label-above field--type-entity_reference field-sectoral-coverage"]/div[2]//div[@class="field field--label-hidden field--type-string field-name"]/div/text()')
            if sectoral_elements:
                Sectoral_coverage_txt = ', '.join([s.strip() for s in sectoral_elements if s.strip()])
            else:
                Sectoral_coverage_txt = ''
        except:
            Sectoral_coverage_txt = ''
        
        # Extract GHGs covered
        try:
            ghg_elements = data_2.xpath('//div[@class="field field--label-above field--type-string_long field-ghgs-covered"]/div[2]/text()')
            if ghg_elements:
                GHGs_covered = ghg_elements[0].replace("\n", ' ').strip()
            else:
                GHGs_covered = ''
        except:
            GHGs_covered = ''
        
        # Extract Offsets and credits
        try:
            offset_elements = data_2.xpath('//div[@class="field field--label-above field--type-string field-offsets-credits-summary"]/div[2]/text()')
            if offset_elements:
                Offsets_credits = offset_elements[0].replace("\n", ' ').strip()
            else:
                Offsets_credits = ''
        except:
            Offsets_credits = ''
        
        # Extract Cap information
        try:
            cap_elements = data_2.xpath('//div[@class="field field--label-above field--type-string field-cap-summary"]/div[2]/text()')
            if cap_elements:
                Cap = cap_elements[0].replace("\n", ' ').strip()
            else:
                Cap = ''
        except:
            Cap = ''
        
        # Apply year filter and save
        try:
            year_int = int(Year) if Year.isdigit() else 0
            if year_int >= MIN_YEAR or Year == '':  # Include ETS systems with no year specified
                with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                    csv_writer = csv.writer(e)
                    csv_writer.writerow([Policy, Year, Country, Policy_Content, URL, Scope, Allocation, Sectoral_coverage_txt, GHGs_covered, Offsets_credits, Cap, 'ICAP'])
                saved_count += 1
                print(f"✅ Saved: {Policy} ({Year})")
            else:
                skipped_count += 1
                print(f"❌ Skipped (before {MIN_YEAR}): {Policy} ({Year})")
        except ValueError:
            # If year is not a valid integer, save it anyway
            with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                csv_writer = csv.writer(e)
                csv_writer.writerow([Policy, Year, Country, Policy_Content, URL, Scope, Allocation, Sectoral_coverage_txt, GHGs_covered, Offsets_credits, Cap, 'ICAP'])
            saved_count += 1
            print(f"⚠️  Saved (non-numeric year): {Policy} ({Year})")
        
        # Progress report every 10 systems
        if (system_index + 1) % 10 == 0:
            print(f"📊 Progress Report - System {system_index + 1}/{len(id_ls)}")
            print(f"   💾 Saved: {saved_count} ETS systems")
            print(f"   ⏭️  Skipped: {skipped_count} ETS systems")
            print("=" * 40)
            
    except Exception as e:
        print(f"❌ Error processing ETS system {single_id}: {e}")
        continue

print(f"\n🎉 ICAP ETS crawling completed!")
print(f"📊 Final Statistics:")
print(f"   💾 Total saved: {saved_count} ETS systems")
print(f"   ⏭️  Total skipped: {skipped_count} ETS systems")
print(f"📂 Output saved to: {output_file}")
