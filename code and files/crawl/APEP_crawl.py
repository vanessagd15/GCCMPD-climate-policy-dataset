"""
APEP Crawler: Asia Pacific Energy Policy Database Scraper

This script crawls the Asia Pacific Energy Policy (APEP) website to extract energy and climate policy information.
It automatically detects the number of pages, iterates through each policy entry, and collects details such as:
- Policy name
- Year
- Country
- Summary/content
- Scope
- Document type
- Economic sector
- Energy types

Policies from MIN_YEAR onwards are saved to a CSV file in the data_new directory. The crawler includes robust error handling,
progress tracking, and respects server load with random delays between requests.

Data is saved either in data_new/APEP.csv

"""

import csv
import requests
from lxml import etree
from fake_useragent import UserAgent
import os
import time
import random

# Configuration: Only extract policies from this year onwards
MIN_YEAR = 2021


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


def get_total_pages():
    """Automatically detect the total number of pages on APEP website"""
    try:
        print("🔍 Detecting total number of pages...")
        first_page_html = get_page('https://policy.asiapacificenergy.org/node')
        
        if first_page_html is None:
            print("⚠️  Could not fetch first page, defaulting to 218 pages")
            return 218
            
        data = etree.HTML(first_page_html)
        
        # Look for pagination elements - try multiple selectors
        pagination_selectors = [
            '//a[contains(@title, "Go to last page")]/@href',  # Last page link
            '//li[@class="pager-last"]/a/@href',               # Pager last
            '//a[contains(text(), "last")]/@href',             # Text "last"
            '//ul[@class="pager"]//a[last()]/@href'            # Last pagination link
        ]
        
        for selector in pagination_selectors:
            last_page_links = data.xpath(selector)
            if last_page_links:
                last_url = last_page_links[0]
                # Extract page number from URL like "?page=217" (0-indexed, so 218 total)
                import re
                page_match = re.search(r'page=(\d+)', last_url)
                if page_match:
                    last_page_num = int(page_match.group(1))
                    total = last_page_num + 1  # Convert from 0-indexed to total count
                    print(f"✅ Detected {total} total pages")
                    return total
        
        print("⚠️  Could not detect pagination, defaulting to 218 pages")
        return 218
        
    except Exception as e:
        print(f"❌ Error detecting total pages: {e}, defaulting to 218")
        return 218


# Create output directory if it doesn't exist
output_dir = os.path.join(os.getcwd(), "data_new")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_file = os.path.join(output_dir, "APEP.csv")

# Check if file exists, if not create with headers
if not os.path.exists(output_file):
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as e:
        csv_writer = csv.writer(e)
        csv_writer.writerow(
            ['Policy', 'Year', 'Country', 'Policy_Content', 'single_url_2', 'Scope', 'Document_Type', 'Economic_Sector',
             'Energy_Types', 'Source'])

# Keep track of progress
start_page = 0  # Start from the beginning
total_pages = get_total_pages()  # Dynamic detection instead of hardcoded 218
saved_count = 0
skipped_count = 0

print(f"🚀 Starting APEP crawler from page {start_page} to {total_pages}")
print(f"📂 Output file: {output_file}")
print(f"📅 Filtering for policies from {MIN_YEAR} onwards")
print("=" * 60)

# Use a while loop for more flexible pagination
current_page = start_page
consecutive_empty_pages = 0
max_empty_pages = 3  # Stop after 3 consecutive empty pages

while current_page < total_pages:
    print(f'=========== 📖 爬取至第{current_page}页 ===========')
    
    if current_page == 0:
        url_1 = 'https://policy.asiapacificenergy.org/node'
    else:
        url_1 = 'https://policy.asiapacificenergy.org/node?page={}'.format(current_page)
    
    res_1 = get_page(url_1)
    if res_1 is None:
        print(f"⚠️  Skipping page {current_page} due to network errors")
        current_page += 1
        continue
        
    try:
        data_1 = etree.HTML(res_1)
        url2_list = data_1.xpath('//a[@rel="tag"]/@href')
        print(f"📊 Found {len(url2_list)} policies on page {current_page}")
        
        if len(url2_list) == 0:
            consecutive_empty_pages += 1
            print(f"⚠️  No policies found on page {current_page} ({consecutive_empty_pages}/{max_empty_pages} consecutive empty pages)")
            
            if consecutive_empty_pages >= max_empty_pages:
                print(f"🏁 Reached end of available pages at page {current_page}")
                break
                
            current_page += 1
            continue
        else:
            consecutive_empty_pages = 0  # Reset counter when we find policies
            
    except Exception as e:
        print(f"❌ Error parsing page {current_page}: {e}")
        current_page += 1
        continue
    
    for policy_index, single_url2_ache in enumerate(url2_list):
        try:
            print(f'*********** 📑 Policy {policy_index + 1}/{len(url2_list)} ************')
            single_url_2 = 'https://policy.asiapacificenergy.org{}'.format(single_url2_ache)
            print(single_url_2)
            
            res_2 = get_page(single_url_2)
            if res_2 is None:
                print(f"⚠️  Skipping policy due to network error")
                continue
                
            data_2 = etree.HTML(res_2)
            
            # Extract policy information
            try:
                Policy_ache = data_2.xpath('//h2[@class="page-header"]/text()')[0]
                if ":" in str(Policy_ache):
                    Country = str(Policy_ache).split(':')[0].replace("\n", ' ').strip()
                    Policy = str(Policy_ache).split(':', 1)[-1].replace("\n", ' ').strip()
                else:
                    Policy = str(Policy_ache).replace("\n", ' ').strip()
                    Country = ''
            except:
                Policy = ''
                Country = ''
                
            if not Policy:
                print("⚠️  No policy name found, skipping...")
                continue
                
            print(f"📋 Policy: {Policy}")
            
            # Initialize other variables
            Year = ''
            Scope = ''
            Document_Type = ''
            Economic_Sector = ''
            Energy_Types = ''
            Policy_Content = ''

            other_data_list = data_2.xpath('//div[@id="bootstrap-panel-body"]/div')
            for single_other in other_data_list:
                try:
                    head_text = single_other.xpath('./div[1]/text()')[0].replace("\n", ' ').strip()
                    
                    # Extract specific fields
                    if 'Effective Start Year:' in head_text:
                        try:
                            Year = single_other.xpath('./div[2]/div/text()')[0].replace("\n", ' ').strip()
                        except:
                            Year = ''
                    elif 'Scope:' in head_text:
                        try:
                            Scope = single_other.xpath('./div[2]/div/text()')[0].replace("\n", ' ').strip()
                        except:
                            Scope = ''
                    elif 'Document Type:' in head_text:
                        try:
                            Document_Type = single_other.xpath('./div[2]/div/text()')[0].replace("\n", ' ').strip()
                        except:
                            Document_Type = ''
                    elif 'Economic Sector:' in head_text:
                        try:
                            Economic_Sector = single_other.xpath('./div[2]/div/text()')[0].replace("\n", ' ').strip()
                        except:
                            Economic_Sector = ''
                    elif 'Energy Types:' in head_text:
                        try:
                            Energy_Types = single_other.xpath('./div[2]/div/text()')[0].replace("\n", ' ').strip()
                        except:
                            Energy_Types = ''
                    elif 'Overall Summary:' in head_text:
                        try:
                            Policy_Content = single_other.xpath('./div[2]/div/text()')[0].replace("\n", ' ').strip()
                        except:
                            Policy_Content = ''
                except:
                    continue
            
            # Apply year filter and save
            try:
                year_int = int(Year) if Year.isdigit() else 0
                if year_int >= MIN_YEAR or Year == '':  # Include policies with no year specified
                    with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                        csv_writer = csv.writer(e)
                        csv_writer.writerow(
                            [Policy, Year, Country, Policy_Content, single_url_2, Scope, Document_Type, Economic_Sector,
                             Energy_Types, 'APEP'])
                    saved_count += 1
                    print(f"✅ Saved: {Policy} ({Year})")
                else:
                    skipped_count += 1
                    print(f"❌ Skipped (before {MIN_YEAR}): {Policy} ({Year})")
            except ValueError:
                # If year is not a valid integer, save it anyway
                with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                    csv_writer = csv.writer(e)
                    csv_writer.writerow(
                        [Policy, Year, Country, Policy_Content, single_url_2, Scope, Document_Type, Economic_Sector,
                         Energy_Types, 'APEP'])
                saved_count += 1
                print(f"⚠️  Saved (non-numeric year): {Policy} ({Year})")
                
        except Exception as e:
            print(f"❌ Error processing policy: {e}")
            continue
    
    # Progress report every 10 pages
    if (current_page + 1) % 10 == 0:
        print(f"📊 Progress Report - Page {current_page + 1}/{total_pages}")
        print(f"   💾 Saved: {saved_count} policies")
        print(f"   ⏭️  Skipped: {skipped_count} policies")
        print("=" * 40)
    
    current_page += 1

print(f"\n🎉 Crawling completed!")
print(f"📊 Final Statistics:")
print(f"   💾 Total saved: {saved_count} policies")
print(f"   ⏭️  Total skipped: {skipped_count} policies")
print(f"📂 Output saved to: {output_file}")
