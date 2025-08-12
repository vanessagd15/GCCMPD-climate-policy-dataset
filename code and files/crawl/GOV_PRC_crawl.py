import time
import requests
from fake_useragent import UserAgent
from lxml import etree
import csv
import json
import os
import random
import re

# Configuration: Only extract policies from this year onwards
MIN_YEAR = 2021

# Create output directory if it doesn't exist
output_dir = os.path.join(os.getcwd(), "data_new")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_file = os.path.join(output_dir, "GOV_PRC.csv")

# Check if file exists, if not create with headers
if not os.path.exists(output_file):
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as e:
        csv_writer = csv.writer(e)
        csv_writer.writerow(['Policy', 'Year', 'Country', 'Policy_Content', 'URL', 'Scope', 'Source'])


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


print(f"🚀 Starting GOV PRC (China) crawler")
print(f"📂 Output file: {output_file}")
print(f"📅 Filtering for policies from {MIN_YEAR} onwards")
print(f"🇨🇳 Source: Chinese Government Policy Database")
print("=" * 60)

# Keep track of progress
saved_count = 0
skipped_count = 0

# Chinese government policy categories (energy and natural resources)
# These categories focus on energy, minerals, coal, oil & gas, and electricity
category_names = {
    '国土资源、能源%5C矿产': 'Natural Resources & Energy - Minerals',
    '国土资源、能源%5C煤炭': 'Natural Resources & Energy - Coal',
    '国土资源、能源%5C石油与天然气': 'Natural Resources & Energy - Oil & Gas',
    '国土资源、能源%5C电力': 'Natural Resources & Energy - Electricity'
}

num_list = ['国土资源、能源%5C矿产', '国土资源、能源%5C煤炭', '国土资源、能源%5C石油与天然气', '国土资源、能源%5C电力']
for category_index, num_d in enumerate(num_list):
    category_name = category_names.get(num_d, num_d)
    print(f'\n=========== 📂 Processing category {category_index + 1}/{len(num_list)}: {category_name} ===========')
    
    n_k = 1
    category_saved = 0
    category_skipped = 0
    
    while True:
        try:
            print(f'🔍 Processing page {n_k} for category: {category_name}')
            
            url = f'http://xxgk.www.gov.cn/search-zhengce/?callback=jQuery1124017801747997612605_1678622720550&mode=smart&sort=relevant&page_index={n_k}&page_size=10&title=&theme={num_d}&_=1678622720562'
            
            # Fetch API data with retry logic
            res_1 = None
            for n_p in range(3):
                try:
                    raw_response = get_page(url)
                    if raw_response is None:
                        continue
                    # Extract JSON from JSONP response
                    res_1 = str(raw_response).split('jQuery1124017801747997612605_1678622720550(')[-1][0:-2]
                    break
                except Exception as e:
                    print(f"⚠️  API parsing attempt {n_p + 1}/3 failed: {e}")
                    continue
            
            if res_1 is None:
                print(f"❌ Failed to fetch data for page {n_k}, skipping...")
                break
                
            # Parse JSON response
            try:
                js_data = json.loads(res_1)
                target_list = js_data.get('data', [])
                print(f"📊 Found {len(target_list)} policies on page {n_k}")
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing error: {e}")
                break
            
            # Check if we've reached the end of results
            if len(target_list) < 1:
                print(f"🏁 Reached end of results for category: {category_name}")
                break
            
            # Process each policy in the current page
            for policy_index, single_target in enumerate(target_list):
                try:
                    print(f'*********** 📑 Policy {policy_index + 1}/{len(target_list)} ************')
                    
                    # Extract basic policy information
                    try:
                        Policy = single_target.get('title', '').strip()
                    except:
                        Policy = ''
                        
                    if not Policy:
                        print("⚠️  No policy title found, skipping...")
                        continue
                        
                    print(f"📋 Policy: {Policy}")
                    
                    # Extract year from writetime
                    try:
                        writetime = str(single_target.get('writetime', ''))
                        # Extract 4-digit year from Chinese date format
                        year_match = re.search(r'(\d{4})', writetime)
                        if year_match:
                            Year = year_match.group(1)
                        else:
                            Year = ''
                    except:
                        Year = ''
                        
                    print(f"📅 Year: {Year}")
                    
                    # Get detailed policy content
                    try:
                        url_2 = single_target.get('url', '')
                        if not url_2:
                            print("⚠️  No policy URL found, skipping...")
                            continue
                            
                        res_2 = get_page(url_2)
                        if res_2 is None:
                            print("⚠️  Could not fetch policy details, skipping...")
                            continue
                            
                        data_2 = etree.HTML(res_2)
                        
                        # Extract policy content
                        try:
                            Policy_Content_ls = data_2.xpath('//td[@class="b12c"]//text()')
                            if Policy_Content_ls:
                                Policy_Content = ''
                                for single_Policy_Content in Policy_Content_ls:
                                    if single_Policy_Content.strip() and single_Policy_Content != '\n':
                                        Policy_Content += single_Policy_Content.replace("\n", ' ').strip() + ' '
                                Policy_Content = Policy_Content.strip()
                            else:
                                # Try alternative content extraction
                                all_text = data_2.xpath('//text()')
                                Policy_Content = ' '.join([t.strip() for t in all_text if t.strip() and len(t.strip()) > 10][:10])
                        except:
                            Policy_Content = ''
                            
                    except Exception as e:
                        print(f"⚠️  Error fetching policy details: {e}")
                        Policy_Content = ''
                        url_2 = single_target.get('url', '')
                    
                    # Apply year filter and save
                    try:
                        year_int = int(Year) if Year.isdigit() else 0
                        if year_int >= MIN_YEAR or Year == '':  # Include policies with no year specified
                            with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                                csv_writer = csv.writer(e)
                                csv_writer.writerow([Policy, Year, 'China', Policy_Content, url_2, 'National', 'GOV_CHN'])
                            saved_count += 1
                            category_saved += 1
                            print(f"✅ Saved: {Policy} ({Year})")
                        else:
                            skipped_count += 1
                            category_skipped += 1
                            print(f"❌ Skipped (before {MIN_YEAR}): {Policy} ({Year})")
                    except ValueError:
                        # If year is not a valid integer, save it anyway
                        with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                            csv_writer = csv.writer(e)
                            csv_writer.writerow([Policy, Year, 'China', Policy_Content, url_2, 'National', 'GOV_CHN'])
                        saved_count += 1
                        category_saved += 1
                        print(f"⚠️  Saved (non-numeric year): {Policy} ({Year})")
                    
                    # Be respectful to the server
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    print(f"❌ Error processing policy {policy_index + 1}: {e}")
                    time.sleep(2)
                    continue
            
            n_k += 1
            
            # Progress report every 5 pages
            if n_k % 5 == 0:
                print(f"📊 Progress Report - Category: {category_name}, Page {n_k}")
                print(f"   💾 Category saved: {category_saved} policies")
                print(f"   ⏭️  Category skipped: {category_skipped} policies")
                print("=" * 40)
                
        except Exception as e:
            print(f"❌ Error processing page {n_k} for category {category_name}: {e}")
            n_k += 1
            continue
    
    print(f"✅ Completed category: {category_name}")
    print(f"   💾 Saved: {category_saved} policies")
    print(f"   ⏭️  Skipped: {category_skipped} policies")

print(f"\n🎉 GOV PRC crawling completed!")
print(f"📊 Final Statistics:")
print(f"   💾 Total saved: {saved_count} policies")
print(f"   ⏭️  Total skipped: {skipped_count} policies")
print(f"📂 Output saved to: {output_file}")
