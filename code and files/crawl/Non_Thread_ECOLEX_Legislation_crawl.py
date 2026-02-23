import csv
import time
import requests
from lxml import etree
from fake_useragent import UserAgent
import os
from pathlib import Path
import re

# Configuration
MIN_YEAR = 2021
MAX_RETRIES = 3
RETRY_DELAY = 2
REQUEST_DELAY = 1.5

# Create output directory
output_dir = Path('../data_new')
output_dir.mkdir(exist_ok=True)
output_file = output_dir / 'ECOLEX_Legislation_NoThreaded.csv'

# Global counters for tracking
saved_count = 0
skipped_count = 0
error_count = 0

# Initialize CSV file with headers
if not os.path.exists(output_file):
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(['Policy', 'Year', 'Country', 'Abstract', 'URL', 'Subject', 'Document_Type', 'Keyword', 'Geographical_area', 'Entry into force notes', 'Source'])


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
                'Accept-Language': 'en-US,en;q=0.9',
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

def extract_legislation_details(single_url2):
    """Extract detailed legislation information with comprehensive error handling"""
    global saved_count, skipped_count
    
    try:
        print(f"🔍 Processing legislation: {single_url2}")
        
        res_2 = get_page(single_url2)
        if res_2 is None:
            print(f"⚠️  Skipping due to network error: {single_url2}")
            return
            
        data_2 = etree.HTML(res_2)
        
        # Extract policy title
        policy_title = ""
        try:
            policy_title = data_2.xpath('//h1/text()')[0].strip()
        except:
            print(f"⚠️  No policy title found for: {single_url2}")
            return
        
        print(f"📋 Policy: {policy_title}")
        
        # Initialize variables for first section data
        year = ""
        document_type = ""
        country = ""
        
        # Extract first section data (header information)
        try:
            other_data_list = data_2.xpath('//header/dl')[0]
            dt_list = other_data_list.xpath('./dt/text()')
            dd_list = other_data_list.xpath('./dd')
            
            for i in range(len(dt_list)):
                try:
                    dt_text = dt_list[i].strip()
                    if 'Country/Territory' in dt_text:
                        country_elements = dd_list[i].xpath('./text()')
                        country = country_elements[0].strip() if country_elements else ""
                    elif 'Document type' in dt_text:
                        doc_type_elements = dd_list[i].xpath('./text()')
                        document_type = doc_type_elements[0].strip() if doc_type_elements else ""
                    elif 'Date' in dt_text:
                        year_elements = dd_list[i].xpath('./span/text()')
                        if year_elements:
                            year_text = year_elements[0].strip()
                            # Extract year from date string
                            year_match = re.search(r'(\d{4})', year_text)
                            year = year_match.group(1) if year_match else ""
                except Exception as e:
                    print(f"⚠️  Error extracting header field {i}: {e}")
                    continue
        except Exception as e:
            print(f"⚠️  Error extracting header data: {e}")
        
        print(f"📅 Year: {year}, 📄 Type: {document_type}, 🌍 Country: {country}")
        
        # Apply year filter
        try:
            year_int = int(year) if year.isdigit() else 0
            if year_int < MIN_YEAR and year != '':
                skipped_count += 1
                print(f"❌ Skipped (before {MIN_YEAR}): {policy_title} ({year})")
                return
        except ValueError:
            pass  # Include policies with non-numeric years
        
        # Initialize variables for second section data
        subject = ""
        keyword = ""
        geographical_area = ""
        entry_into_force_notes = ""
        
        # Extract second section data (details)
        try:
            other_data_list_2 = data_2.xpath('//section[@id="details"]/dl')[0]
            dt_list_2 = other_data_list_2.xpath('./dt/text()')
            dd_list_2 = other_data_list_2.xpath('./dd')
            
            for i in range(len(dt_list_2)):
                try:
                    dt_text = dt_list_2[i].strip()
                    if 'Subject' in dt_text:
                        subject_elements = dd_list_2[i].xpath('./text()')
                        subject = subject_elements[0].strip() if subject_elements else ""
                    elif 'Keyword' in dt_text:
                        keyword_elements = dd_list_2[i].xpath('./span/text()')
                        if keyword_elements:
                            keyword = ', '.join([kw.strip() for kw in keyword_elements])
                    elif 'Geographical area' in dt_text:
                        geo_elements = dd_list_2[i].xpath('./text()')
                        geographical_area = geo_elements[0].strip() if geo_elements else ""
                    elif 'Entry into force notes' in dt_text:
                        entry_elements = dd_list_2[i].xpath('./text()')
                        entry_into_force_notes = entry_elements[0].strip() if entry_elements else ""
                except Exception as e:
                    print(f"⚠️  Error extracting detail field {i}: {e}")
                    continue
        except Exception as e:
            print(f"⚠️  Error extracting detail data: {e}")
        
        print(f"🏷️  Subject: {subject}, 📍 Geo: {geographical_area}")
        
        # Extract abstract with multiple fallbacks
        abstract = ""
        try:
            abstract_elements = data_2.xpath('//p[@class="abstract"]/text()')
            if abstract_elements:
                abstract = abstract_elements[0].strip()
            else:
                # Try alternative selector
                comment_elements = data_2.xpath('//p[@class="comment"]/text()')
                if comment_elements:
                    abstract = comment_elements[0].strip()
        except Exception as e:
            print(f"⚠️  Error extracting abstract: {e}")
        
        # Limit abstract length for CSV compatibility
        if len(abstract) > 5000:
            abstract = abstract[:5000] + '...'
        
        # Clean all text fields for CSV compatibility
        policy_data = [
            policy_title.replace('\n', ' ').replace('\r', ' ').strip(),
            year,
            country.replace('\n', ' ').replace('\r', ' ').strip(),
            abstract.replace('\n', ' ').replace('\r', ' ').strip(),
            single_url2,
            subject.replace('\n', ' ').replace('\r', ' ').strip(),
            document_type.replace('\n', ' ').replace('\r', ' ').strip(),
            keyword.replace('\n', ' ').replace('\r', ' ').strip(),
            geographical_area.replace('\n', ' ').replace('\r', ' ').strip(),
            entry_into_force_notes.replace('\n', ' ').replace('\r', ' ').strip(),
            'ECOLEX'
        ]
        
        # Write to CSV
        with open(output_file, 'a+', encoding='utf-8-sig', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(policy_data)
        
        saved_count += 1
        print(f"✅ Saved: {policy_title} ({year})")
        print(f"📊 Progress: {saved_count} saved, {skipped_count} skipped")
        
    except Exception as e:
        print(f"❌ Error processing legislation details: {e}")
        error_count += 1


def main():
    """Main crawling function with enhanced error handling"""
    print("=" * 60)
    print("⚖️  ECOLEX Legislation Crawler (Non-Threaded)")
    print("🎯 Enhanced version with year filtering and robust error handling")
    print("=" * 60)
    print(f"📂 Output file: {output_file}")
    print(f"📅 Filtering policies from {MIN_YEAR} onwards")
    
    # Construct initial URL with updated date range
    base_url = f'https://www.ecolex.org/result/?q=&type=legislation&xsubjects=Agricultural+%26+rural+development&xsubjects=Air+%26+atmosphere&xsubjects=Energy&xsubjects=Environment+gen.&xsubjects=Forestry&xsubjects=General&xsubjects=Land+%26+soil&xsubjects=Mineral+resources&xdate_min={MIN_YEAR}&xdate_max=2024'
    
    print(f"🚀 Starting ECOLEX legislation crawl...")
    print(f"🔗 Base URL: {base_url}")
    
    try:
        # Get total number of pages
        print("📊 Determining total number of pages...")
        res = get_page(base_url)
        if res is None:
            print("❌ Failed to fetch initial page. Please check your internet connection.")
            return
            
        data = etree.HTML(res)
        page_elements = data.xpath("//a[contains(@class, 'btn btn-sm btn-default')][last()-1]/text()")
        
        if not page_elements:
            print("❌ Could not determine total number of pages")
            return
            
        total_pages = int(page_elements[0])
        print(f"📄 Found {total_pages} total pages to process")
        
        # Process each page
        for page_num in range(1, total_pages + 1):
            try:
                print(f"\n📖 Processing page {page_num}/{total_pages}")
                
                # Construct page URL
                if page_num == 1:
                    page_url = base_url
                else:
                    page_url = f'https://www.ecolex.org/result/?type=legislation&xsubjects=Agricultural+%26+rural+development&xsubjects=Air+%26+atmosphere&xsubjects=Energy&xsubjects=Environment+gen.&xsubjects=Forestry&xsubjects=General&xsubjects=Land+%26+soil&xsubjects=Mineral+resources&xdate_min={MIN_YEAR}&xdate_max=2024&page={page_num}'
                
                # Fetch page content
                res_1 = get_page(page_url)
                if res_1 is None:
                    print(f"⚠️  Skipping page {page_num} due to network error")
                    continue
                
                data_1 = etree.HTML(res_1)
                url2_list = data_1.xpath('//h3[@class="search-result-title"]/a/@href')
                
                print(f"📋 Found {len(url2_list)} legislation entries on page {page_num}")
                
                # Process each legislation entry
                for entry_index, single_url2_path in enumerate(url2_list):
                    try:
                        print(f"\n🔍 Entry {entry_index + 1}/{len(url2_list)} on page {page_num}")
                        
                        single_url2 = f'https://www.ecolex.org{single_url2_path}'
                        
                        # Extract legislation details
                        extract_legislation_details(single_url2)
                        
                        # Respectful delay between requests
                        time.sleep(REQUEST_DELAY)
                        
                    except Exception as e:
                        print(f"⚠️  Error processing entry {entry_index + 1}: {e}")
                        continue
                        
            except Exception as e:
                print(f"❌ Error processing page {page_num}: {e}")
                continue
    
    except Exception as e:
        print(f"❌ Critical error during crawling: {e}")
    
    # Final summary
    print(f"\n🎉 ECOLEX legislation extraction completed!")
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
