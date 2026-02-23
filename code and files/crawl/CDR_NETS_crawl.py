import time
import requests
from fake_useragent import UserAgent
from lxml import etree
import csv
import re
import os
import random

# Configuration: Only extract policies from this year onwards
MIN_YEAR = 2021

# Create output directory if it doesn't exist
output_dir = os.path.join(os.getcwd(), "data_new")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_file = os.path.join(output_dir, "CDR_NETS.csv")

# Check if file exists, if not create with headers
if not os.path.exists(output_file):
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as e:
        csv_writer = csv.writer(e)
        csv_writer.writerow(['Policy', 'Year', 'Keyword', 'Policy_Content', 'URL', 'Type', 'Source'])


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


print(f"🚀 Starting CDR NETS crawler")
print(f"📂 Output file: {output_file}")
print(f"📅 Filtering for policies from {MIN_YEAR} onwards")
print("=" * 60)

# Keep track of progress
saved_count = 0
skipped_count = 0


try:
    res_1 = get_page('https://cdrlaw.org/technical-pathway/negative-emission-technologies/?_cdr_res_type=pleg%2Cdec%2Cpop%2Cpol%2Celeg%2Cnew%2Chear')
    if res_1 is None:
        print("❌ Could not fetch initial page, exiting...")
        exit(1)
        
    res_1 = str(res_1).replace('\\', '')
    
    # Extract maximum page number with better error handling
    try:
        max_page_match = re.search('<a class=\"facetwp-page last\" data-page=\".*?\"', res_1, re.S)
        if max_page_match:
            max_page = int(max_page_match.group(0).split('data-page="')[-1].split('"')[0])
            print(f'✅ Detected {max_page} maximum pages')
        else:
            print("⚠️  Could not detect pagination, defaulting to 10 pages")
            max_page = 10
    except (ValueError, AttributeError) as e:
        print(f"❌ Error detecting max pages: {e}, defaulting to 10")
        max_page = 10

    for i in range(1, max_page + 1):
        print(f'=========== 📖 Processing page {i}/{max_page} ===========')
        
        if i == 1:
            url_2 = 'https://cdrlaw.org/technical-pathway/negative-emission-technologies/?_cdr_res_type=pleg%2Cdec%2Cpop%2Cpol%2Celeg%2Cnew%2Chear'
        else:
            url_2 = f'https://cdrlaw.org/technical-pathway/negative-emission-technologies/?_cdr_res_type=pleg%2Cdec%2Cpop%2Cpol%2Celeg%2Cnew%2Chear&_paged={i}'
        
        res_2 = get_page(url_2)
        if res_2 is None:
            print(f"⚠️  Skipping page {i} due to network errors")
            continue
            
        try:
            data_2 = etree.HTML(res_2)
            url_list = data_2.xpath('//article/h2/a/@href')
            print(f"📊 Found {len(url_list)} policies on page {i}")
            
            if len(url_list) == 0:
                print("⚠️  No policies found on this page, continuing...")
                continue
                
        except Exception as e:
            print(f"❌ Error parsing page {i}: {e}")
            continue
            
        for policy_index, url_3 in enumerate(url_list):
            try:
                print(f'*********** 📑 Policy {policy_index + 1}/{len(url_list)} ************')
                print(url_3)
                
                res_3 = get_page(url_3)
                if res_3 is None:
                    print(f"⚠️  Skipping policy due to network error")
                    continue
                    
                data_3 = etree.HTML(res_3)
                
                # Extract policy information
                try:
                    Policy = data_3.xpath('//h1/text()')[0]
                except:
                    Policy = ''
                    
                if not Policy:
                    print("⚠️  No policy name found, skipping...")
                    continue
                    
                print(f"📋 Policy: {Policy}")
                
                try:
                    Year = data_3.xpath('//div[@class="resource-year"]/text()')[0].strip()
                except:
                    Year = ''
                    
                try:
                    Keyword = ','.join(data_3.xpath('//div[@class="cdr_resource_keyword"]/a/text()'))
                except:
                    Keyword = ''
                    
                try:
                    Policy_Content_ls = data_3.xpath('//div[@class="entry-content"]/p//text()')
                    Policy_Content_txt = ''.join(Policy_Content_ls)
                except:
                    Policy_Content_txt = ''
                    
                try:
                    Type = data_3.xpath('//div[@class="resource-type"]/text()')[0].replace("\n", ' ').strip()
                except:
                    Type = ''
                    
                Source = 'CDR_NETS'
                
                # Apply year filter and save
                try:
                    year_int = int(Year) if Year.isdigit() else 0
                    if year_int >= MIN_YEAR or Year == '':  # Include policies with no year specified
                        with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                            csv_writer = csv.writer(e)
                            csv_writer.writerow([Policy, Year, Keyword, Policy_Content_txt, url_3, Type, Source])
                        saved_count += 1
                        print(f"✅ Saved: {Policy} ({Year})")
                    else:
                        skipped_count += 1
                        print(f"❌ Skipped (before {MIN_YEAR}): {Policy} ({Year})")
                except ValueError:
                    # If year is not a valid integer, save it anyway
                    with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                        csv_writer = csv.writer(e)
                        csv_writer.writerow([Policy, Year, Keyword, Policy_Content_txt, url_3, Type, Source])
                    saved_count += 1
                    print(f"⚠️  Saved (non-numeric year): {Policy} ({Year})")
                    
            except Exception as e:
                print(f"❌ Error processing policy: {e}")
                continue
        
        # Progress report every 5 pages
        if i % 5 == 0:
            print(f"📊 Progress Report - Page {i}/{max_page}")
            print(f"   💾 Saved: {saved_count} policies")
            print(f"   ⏭️  Skipped: {skipped_count} policies")
            print("=" * 40)

except Exception as e:
    print(f"❌ Critical error in main crawling loop: {e}")

print(f"\n🎉 CDR NETS crawling completed!")
print(f"📊 Final Statistics:")
print(f"   💾 Total saved: {saved_count} policies")
print(f"   ⏭️  Total skipped: {skipped_count} policies")
print(f"📂 Output saved to: {output_file}")
