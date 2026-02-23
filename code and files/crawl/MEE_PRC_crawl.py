import time
import requests
from fake_useragent import UserAgent
from lxml import etree
import csv
import os
from pathlib import Path

# Configuration
MIN_YEAR = 2021
MAX_RETRIES = 3
RETRY_DELAY = 2
REQUEST_DELAY = 3  # Increased delay for Chinese government site

# Create output directory
output_dir = Path('../data_new')
output_dir.mkdir(exist_ok=True)
output_file = output_dir / 'MEE_PRC_policies.csv'

# Global counters for tracking
saved_count = 0
skipped_count = 0
error_count = 0

# Initialize CSV file with headers
if not os.path.exists(output_file):
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(['Policy', 'Year', 'Country', 'Policy_Content', 'URL', 'Scope', 'Source'])


def get_page(url):
    """Fetch page content with robust error handling and retry logic"""
    global error_count
    
    print(f"🌐 Fetching: {url}")
    
    for attempt in range(MAX_RETRIES):
        try:
            ua = UserAgent()
            usr_ag = ua.random
            headers = {
                'User-Agent': usr_ag,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            response = requests.get(
                url, 
                headers=headers, 
                timeout=(10, 30),  # (connection, read) timeout
                verify=True
            )
            response.raise_for_status()
            
            print(f"✅ Status code: {response.status_code}")
            response.encoding = 'utf-8'
            return response.text
            
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout on attempt {attempt + 1}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        except requests.exceptions.ConnectionError:
            print(f"🔌 Connection error on attempt {attempt + 1}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        except requests.exceptions.HTTPError as e:
            print(f"❌ HTTP error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        except Exception as e:
            print(f"❌ Unexpected error on attempt {attempt + 1}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
    
    print(f"❌ Failed to fetch after {MAX_RETRIES} attempts: {url}")
    error_count += 1
    return None


def extract_policy_details(single_target, single_url2):
    """Extract detailed policy information with comprehensive error handling"""
    global saved_count, skipped_count
    
    try:
        print(f"🔍 Processing policy details from: {single_url2}")
        
        res_2 = get_page(single_url2)
        if res_2 is None:
            print(f"⚠️  Skipping due to network error: {single_url2}")
            return
            
        data_2 = etree.HTML(res_2)
        
        # Extract policy title with multiple fallbacks
        policy_title = ""
        try:
            policy_title = data_2.xpath('//li[@class="first"]/div/p/text()')[0].strip()
        except:
            try:
                policy_title = data_2.xpath('//h2[@class="neiright_Title"]/text()')[0].strip()
            except:
                try:
                    policy_title = data_2.xpath('//title/text()')[0].split('-')[0].strip()
                except:
                    policy_title = ""
        
        # Extract year
        year = ""
        try:
            year_text = str(single_target.xpath('./td[1]/span/text()')[0])
            year = year_text.split('-')[0]
        except:
            try:
                # Try to extract from URL or content
                year_candidates = data_2.xpath('//text()[contains(., "20")]')
                for candidate in year_candidates:
                    import re
                    year_match = re.search(r'20\d{2}', candidate)
                    if year_match:
                        year = year_match.group()
                        break
            except:
                year = ""
        
        # Apply year filter
        try:
            year_int = int(year) if year.isdigit() else 0
            if year_int < MIN_YEAR and year != '':
                skipped_count += 1
                print(f"❌ Skipped (before {MIN_YEAR}): {policy_title} ({year})")
                return
        except ValueError:
            pass  # Include policies with non-numeric years
        
        # Extract policy content with multiple fallbacks
        policy_content = ""
        try:
            # First attempt: main content div
            content_elements = data_2.xpath('//div[@id="print_html"]//text()')
            if content_elements and any(elem.strip() for elem in content_elements):
                policy_content = ''.join([elem.replace("\n", ' ').strip() for elem in content_elements if elem.strip()])
            else:
                raise Exception("No content in first selector")
        except:
            try:
                # Second attempt: alternative content div
                content_elements = data_2.xpath('//div[@class="neiright_JPZ_GK_CP"]//text()')
                if content_elements and any(elem.strip() for elem in content_elements):
                    policy_content = ''.join([elem.replace("\n", ' ').strip() for elem in content_elements if elem.strip()])
                else:
                    raise Exception("No content in second selector")
            except:
                try:
                    # Third attempt: any main content area
                    content_elements = data_2.xpath('//div[contains(@class,"content")]//text()')
                    if content_elements:
                        policy_content = ''.join([elem.replace("\n", ' ').strip() for elem in content_elements if elem.strip()])
                except:
                    policy_content = ""
        
        # Limit content length for CSV compatibility
        if len(policy_content) > 10000:
            policy_content = policy_content[:10000] + '...'
        
        # Clean content for CSV
        policy_content = ' '.join(policy_content.split())  # Remove extra whitespace
        
        # Only save if we have a policy title
        if policy_title.strip():
            # Prepare data for CSV
            policy_data = [
                policy_title,
                year,
                'China',
                policy_content,
                single_url2,
                'National',
                'MEE_PRC'
            ]
            
            # Write to CSV
            with open(output_file, 'a+', encoding='utf-8-sig', newline='') as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(policy_data)
            
            saved_count += 1
            print(f"✅ Saved: {policy_title} ({year})")
            print(f"📊 Progress: {saved_count} saved, {skipped_count} skipped")
        else:
            print(f"⚠️  Skipping: No policy title found for {single_url2}")
            
    except Exception as e:
        print(f"❌ Error processing policy details: {e}")
        error_count += 1


def main():
    """Main crawling function with enhanced error handling"""
    print("=" * 60)
    print("🇨🇳 MEE PRC Climate Policy Crawler")
    print("🎯 Enhanced version with year filtering and robust error handling")
    print("=" * 60)
    print(f"📂 Output file: {output_file}")
    print(f"📅 Filtering policies from {MIN_YEAR} onwards")
    
    # Target directory numbers for different policy categories
    num_list = [168, 169, 170, 174, 177, 178, 180, 182, 183, 184, 185, 186, 187, 188, 189]
    
    print(f"🚀 Starting to crawl {len(num_list)} policy categories...")
    
    for category_index, num_d in enumerate(num_list):
        print(f"\n📂 Processing category {category_index + 1}/{len(num_list)}: Directory {num_d}")
        
        page_num = 0
        
        while True:
            try:
                print(f"\n📖 Category {num_d} - Page {page_num + 1}")
                
                # Construct URL based on page number
                if page_num == 0:
                    url = f'https://www.mee.gov.cn/xxgk2018/160/167/{num_d}/index_6700.html'
                else:
                    url = f'https://www.mee.gov.cn/xxgk2018/160/167/{num_d}/index_6700_{page_num}.html'
                
                # Fetch page content
                res_1 = get_page(url)
                if res_1 is None:
                    print(f"⚠️  Failed to fetch page, moving to next category")
                    break
                
                # Parse page content
                data_1 = etree.HTML(res_1)
                url_2_list = data_1.xpath('//div[@class="iframe-list"]/table//tr')
                
                print(f"📋 Found {len(url_2_list)} policy entries on this page")
                page_num += 1
                
                # If no policies found, move to next category
                if len(url_2_list) < 1:
                    print(f"📄 No more pages in category {num_d}")
                    break
                
                # Process each policy on the page
                for policy_index, single_target in enumerate(url_2_list):
                    try:
                        # Extract policy detail URL
                        policy_url_element = single_target.xpath('./td[2]/a/@href')
                        if not policy_url_element:
                            continue
                            
                        single_url2 = 'https://www.mee.gov.cn/xxgk2018' + str(policy_url_element[0]).replace('../../..', '')
                        
                        print(f"🔗 Policy {policy_index + 1}/{len(url_2_list)}: {single_url2}")
                        
                        # Extract policy details
                        extract_policy_details(single_target, single_url2)
                        
                        # Respectful delay for Chinese government server
                        time.sleep(REQUEST_DELAY)
                        
                    except Exception as e:
                        print(f"⚠️  Error processing policy {policy_index + 1}: {e}")
                        time.sleep(REQUEST_DELAY)
                        continue
                        
            except Exception as e:
                print(f"❌ Error processing page {page_num} of category {num_d}: {e}")
                break
    
    # Final summary
    print(f"\n🎉 MEE PRC policy extraction completed!")
    print(f"📊 Final Statistics:")
    print(f"   ✅ Total saved: {saved_count} policies")
    print(f"   ⏭️  Total skipped (before {MIN_YEAR}): {skipped_count} policies")
    print(f"   ❌ Total errors: {error_count} requests")
    print(f"📂 Output saved to: {output_file}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Crawling interrupted by user")
        print(f"📊 Progress so far:")
        print(f"   ✅ Total saved: {saved_count} policies")
        print(f"   ⏭️  Total skipped: {skipped_count} policies")
        print(f"   ❌ Total errors: {error_count} requests")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        print(f"📊 Progress before error:")
        print(f"   ✅ Total saved: {saved_count} policies")
        print(f"   ⏭️  Total skipped: {skipped_count} policies")
        print(f"   ❌ Total errors: {error_count} requests")
