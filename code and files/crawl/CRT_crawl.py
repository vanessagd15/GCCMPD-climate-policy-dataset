import csv
from lxml import etree
import requests
from fake_useragent import UserAgent
import re
import json
import os
import time
import random

# Configuration: Only extract policies from this year onwards
MIN_YEAR = 2021

# Create output directory if it doesn't exist
output_dir = os.path.join(os.getcwd(), "data_new")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_file = os.path.join(output_dir, "CRT.csv")

# Check if file exists, if not create with headers
if not os.path.exists(output_file):
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as e:
        csv_writer = csv.writer(e)
        csv_writer.writerow(['Policy', 'Year', 'Country', 'Policy_Content', 'URL', 'Scope', 'Explanation', 'Agency', 'Source'])


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


print(f"🚀 Starting CRT crawler")
print(f"📂 Output file: {output_file}")
print(f"📅 Filtering for policies from {MIN_YEAR} onwards")
print("=" * 60)

# Keep track of progress
saved_count = 0
skipped_count = 0


# Request main page with all data
url = 'https://climate.law.columbia.edu/content/climate-reregulation-tracker'
print("🔍 Fetching main page with embedded data...")

try:
    res_1 = get_page(url)
    if res_1 is None:
        print("❌ Could not fetch main page, exiting...")
        exit(1)
    
    print("✅ Successfully fetched main page")
    
    # Extract Explanation library for department parameter parsing
    print("📋 Parsing department mappings...")
    try:
        explanation_match = re.findall('var services_dept_data = .*?;var services_aud_data', res_1)
        if not explanation_match:
            print("❌ Could not find department data")
            exit(1)
            
        Explanation_key_list = eval(explanation_match[0].replace('var services_dept_data = ', '').replace(';var services_aud_data', ''))
        
        # Build Explanation mapping (numeric ID to department name)
        dict_Explanation = {}
        for e_1 in Explanation_key_list:
            dict_Explanation[e_1['id']] = e_1['label']
        print(f"✅ Parsed {len(dict_Explanation)} department mappings")
        
    except Exception as e:
        print(f"❌ Error parsing department data: {e}")
        exit(1)

    # Extract Agency library for agency parameter parsing
    print("🏛️  Parsing agency mappings...")
    try:
        agency_match = re.search('var services_aud_data = .*?;var services_cat_data', res_1, re.S)
        if not agency_match:
            print("❌ Could not find agency data")
            exit(1)
            
        Agency_key_list = eval(agency_match.group(0).replace('var services_aud_data = ', '').replace(';var services_cat_data', ''))
        
        # Build Agency mapping (numeric ID to agency name)
        dict_Agency = {}
        for e_2 in Agency_key_list:
            dict_Agency[e_2['id']] = e_2['label']
        print(f"✅ Parsed {len(dict_Agency)} agency mappings")
        
    except Exception as e:
        print(f"❌ Error parsing agency data: {e}")
        exit(1)

    # Extract main policy data
    print("📊 Parsing main policy data...")
    try:
        data_match = re.search('var services_data = .*?;var services_dept_data', res_1, re.S)
        if not data_match:
            print("❌ Could not find main policy data")
            exit(1)
            
        data_1 = data_match.group(0).replace('var services_data = ', '').replace(';var services_dept_data', '')
        js_data = json.loads(data_1)
        print(f"✅ Found {len(js_data)} policies to process")
        
    except Exception as e:
        print(f"❌ Error parsing main policy data: {e}")
        exit(1)

except Exception as e:
    print(f"❌ Critical error in initial data extraction: {e}")
    exit(1)
# Process each policy
print("🔄 Processing individual policies...")
for policy_index, single_data in enumerate(js_data):
    try:
        print(f'=========== 📑 Policy {policy_index + 1}/{len(js_data)} ===========')
        
        # Fixed data for all CRT policies
        Country = 'USA'
        Scope = 'National'
        Source = 'Climate-Reregulation-Tracker'
        
        # Extract basic policy information
        try:
            Policy = single_data['title']
            if not Policy:
                print("⚠️  No policy name found, skipping...")
                continue
        except:
            print("⚠️  No policy name found, skipping...")
            continue
            
        print(f"📋 Policy: {Policy}")

        try:
            Year = str(single_data['date']).split('-')[0].replace('\"', '')
        except:
            Year = ''
        print(f"📅 Year: {Year}")

        try:
            Summary = single_data['summary']
        except:
            Summary = ''

        # Parse Explanation parameter (departments)
        try:
            Explanation_num_list = list(single_data['departments_id'])
            Explanation_txt = ''
            for i, single_Explanation_num in enumerate(Explanation_num_list):
                if i > 0:
                    Explanation_txt += ','
                Explanation_txt += dict_Explanation.get(single_Explanation_num, '')
        except:
            Explanation_txt = ''
        print(f"🏢 Departments: {Explanation_txt}")
        
        # Parse Agency parameter
        try:
            Agency_num_list = list(single_data['groups_id'])
            Agency_txt = ''
            for i, single_Agency_num in enumerate(Agency_num_list):
                if i > 0:
                    Agency_txt += ','
                Agency_txt += dict_Agency.get(single_Agency_num, '')
        except:
            Agency_txt = ''
        print(f"🏛️  Agencies: {Agency_txt}")
        
        # Build individual policy URL and get detailed content
        try:
            url_t = 'https://climate.law.columbia.edu' + single_data['path']
            print(f"🔗 URL: {url_t}")
            
            res_2 = get_page(url_t)
            if res_2 is None:
                print("⚠️  Could not fetch policy details, using summary only")
                main_txt = ''
            else:
                data_2 = etree.HTML(res_2)
                main_txt = ''
                text_list = data_2.xpath('//div[@class="field field--name-field-cu-wysiwyg field--type-text-long field--label-hidden field--item"]//text()')
                for single_text in text_list:
                    if single_text.strip():
                        main_txt += single_text.strip() + ' '
        except Exception as e:
            print(f"⚠️  Error fetching policy details: {e}, using summary only")
            main_txt = ''
            url_t = ''
        
        Policy_Content = Summary + '\n' + main_txt if main_txt else Summary
        
        # Apply year filter and save
        try:
            year_int = int(Year) if Year.isdigit() else 0
            if year_int >= MIN_YEAR or Year == '':  # Include policies with no year specified
                with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                    csv_writer = csv.writer(e)
                    csv_writer.writerow([Policy, Year, Country, Policy_Content, url_t, Scope, Explanation_txt, Agency_txt, Source])
                saved_count += 1
                print(f"✅ Saved: {Policy} ({Year})")
            else:
                skipped_count += 1
                print(f"❌ Skipped (before {MIN_YEAR}): {Policy} ({Year})")
        except ValueError:
            # If year is not a valid integer, save it anyway
            with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                csv_writer = csv.writer(e)
                csv_writer.writerow([Policy, Year, Country, Policy_Content, url_t, Scope, Explanation_txt, Agency_txt, Source])
            saved_count += 1
            print(f"⚠️  Saved (non-numeric year): {Policy} ({Year})")
            
    except Exception as e:
        print(f"❌ Error processing policy {policy_index + 1}: {e}")
        continue
    
    # Progress report every 10 policies
    if (policy_index + 1) % 10 == 0:
        print(f"📊 Progress Report - Policy {policy_index + 1}/{len(js_data)}")
        print(f"   💾 Saved: {saved_count} policies")
        print(f"   ⏭️  Skipped: {skipped_count} policies")
        print("=" * 40)

print(f"\n🎉 CRT crawling completed!")
print(f"📊 Final Statistics:")
print(f"   💾 Total saved: {saved_count} policies")
print(f"   ⏭️  Total skipped: {skipped_count} policies")
print(f"📂 Output saved to: {output_file}")
