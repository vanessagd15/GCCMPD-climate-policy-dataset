import csv
import time
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from lxml import etree
from fake_useragent import UserAgent

# Configuration: Only extract policies from this year onwards
MIN_YEAR = 2021

# Create output directory if it doesn't exist
output_dir = os.path.join(os.getcwd(), "data_new")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_file = os.path.join(output_dir, "ECOLEX_Legislation.csv")

# Global counters for thread-safe tracking
saved_count = 0
skipped_count = 0


def get_page(url, max_retries=3):
    """Get page content with retry logic and better error handling"""
    for attempt in range(max_retries):
        try:
            ua = UserAgent()
            usr_ag = ua.random
            headers = {'User-Agent': usr_ag}
            
            # Increase timeout and add random delay
            response = requests.get(url, headers=headers, timeout=10)
            # print(f"Status: {response.status_code}")
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


def parse_detail(url):
    """Parse individual policy details with enhanced error handling and year filtering"""
    global saved_count, skipped_count
    result = []
    
    try:
        single_url_2 = 'https://www.ecolex.org{}'.format(url)
        res_2 = get_page(single_url_2)
        
        if res_2 is None:
            print(f"⚠️  Skipping {url} due to network error")
            return None
            
        data_2 = etree.HTML(res_2)
        
        # Extract Policy name
        try:
            Policy = data_2.xpath('//h1/text()')[0].strip()
        except:
            Policy = ''
            
        if not Policy:
            print(f"⚠️  No policy name found for {url}, skipping...")
            return None
        
        # Initialize variables
        Year = ''
        Document_Type = ''
        Country = ''
        
        # Extract first section data (Country, Document Type, Date)
        try:
            other_data_list = data_2.xpath('//header/dl')[0]
            dt_list = other_data_list.xpath('./dt/text()')
            dd_list = other_data_list.xpath('./dd')
            
            for s_1 in range(len(dt_list)):
                try:
                    if 'Country/Territory' in dt_list[s_1]:
                        Country = dd_list[s_1].xpath('./text()')[0].replace("\n", ' ').strip()
                    elif 'Document type' in dt_list[s_1]:
                        Document_Type = dd_list[s_1].xpath('./text()')[0].replace("\n", ' ').strip()
                    elif 'Date' in dt_list[s_1]:
                        date_elements = dd_list[s_1].xpath('./span/text()')
                        if date_elements:
                            Year = date_elements[0].replace("\n", ' ').strip()
                        else:
                            # Try alternative date extraction
                            date_text = dd_list[s_1].xpath('./text()')
                            if date_text:
                                Year = date_text[0].replace("\n", ' ').strip()
                except Exception as e:
                    continue
        except Exception as e:
            pass

        # Extract second section data (Subject, Keywords, etc.)
        Subject = ''
        Keyword = ''
        Geographical_area = ''
        Entry_into_force_notes = ''
        
        try:
            other_data_list_2 = data_2.xpath('//section[@id="details"]/dl')[0]
            dt_list_2 = other_data_list_2.xpath('./dt/text()')
            dd_list_2 = other_data_list_2.xpath('./dd')
            
            for s_2 in range(len(dt_list_2)):
                try:
                    if 'Subject' in dt_list_2[s_2]:
                        subject_elements = dd_list_2[s_2].xpath('./text()')
                        if subject_elements:
                            Subject = subject_elements[0].strip()
                    elif 'Keyword' in dt_list_2[s_2]:
                        Keyword_ls = dd_list_2[s_2].xpath('.//text()')
                        Keyword = ', '.join([k.strip() for k in Keyword_ls if k.strip()])
                    elif 'Geographical area' in dt_list_2[s_2]:
                        geo_elements = dd_list_2[s_2].xpath('./text()')
                        if geo_elements:
                            Geographical_area = geo_elements[0].strip()
                    elif 'Entry into force notes' in dt_list_2[s_2]:
                        entry_elements = dd_list_2[s_2].xpath('./text()')
                        if entry_elements:
                            Entry_into_force_notes = entry_elements[0].strip()
                except Exception as e:
                    continue
        except Exception as e:
            pass

        # Extract Abstract/Policy Content
        Abstract = ''
        try:
            abstract_elements = data_2.xpath('//p[@class="abstract"]/text()')
            if abstract_elements:
                Abstract = abstract_elements[0].strip()
            else:
                # Try alternative abstract extraction
                comment_elements = data_2.xpath('//p[@class="comment"]/text()')
                if comment_elements:
                    Abstract = comment_elements[0].strip()
        except Exception as e:
            pass

        # Apply year filter
        try:
            # Extract year from various date formats
            year_int = 0
            if Year:
                # Try to extract 4-digit year from date string
                import re
                year_match = re.search(r'\b(19|20)\d{2}\b', Year)
                if year_match:
                    year_int = int(year_match.group())
                
            if year_int >= MIN_YEAR or Year == '':  # Include policies with no year specified
                result = [Policy, Year, Country, Abstract, single_url_2, Subject, Document_Type, Keyword,
                         Geographical_area, Entry_into_force_notes, 'ECOLEX_Legislation']
                saved_count += 1
                print(f"✅ Saved: {Policy} ({Year})")
                return result
            else:
                skipped_count += 1
                print(f"❌ Skipped (before {MIN_YEAR}): {Policy} ({Year})")
                return None
                
        except Exception as e:
            # If year parsing fails, save it anyway
            result = [Policy, Year, Country, Abstract, single_url_2, Subject, Document_Type, Keyword,
                     Geographical_area, Entry_into_force_notes, 'ECOLEX_Legislation']
            saved_count += 1
            print(f"⚠️  Saved (year parsing failed): {Policy} ({Year})")
            return result

    except Exception as e:
        print(f"❌ Error parsing {url}: {e}")
        return None


# Check if file exists, if not create with headers
if not os.path.exists(output_file):
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as e:
        csv_writer = csv.writer(e)
        csv_writer.writerow(['Policy', 'Year', 'Country', 'Policy_Content', 'URL', 'Subject', 'Document_Type', 'Keyword', 'Geographical_area', 'Entry into force notes', 'Source'])

print(f"🚀 Starting ECOLEX Legislation crawler")
print(f"📂 Output file: {output_file}")
print(f"📅 Filtering for policies from {MIN_YEAR} onwards")
print("=" * 60)

# Get initial page to determine total pages
print("🔍 Detecting total number of pages...")
initial_url = 'https://www.ecolex.org/result/?q=&type=legislation&xsubjects=Agricultural+%26+rural+development&xsubjects=Air+%26+atmosphere&xsubjects=Energy&xsubjects=Environment+gen.&xsubjects=Forestry&xsubjects=General&xsubjects=Land+%26+soil&xsubjects=Mineral+resources&xdate_min=1900&xdate_max=2021'

try:
    res = get_page(initial_url)
    if res is None:
        print("❌ Could not fetch initial page, exiting...")
        exit(1)
        
    data = etree.HTML(res)
    page_elements = data.xpath("//a[contains(@class, 'btn btn-sm btn-default')][last()-1]/text()")
    
    if page_elements:
        page_number = int(page_elements[0])
        print(f"✅ Detected {page_number} total pages")
    else:
        print("⚠️  Could not detect pagination, defaulting to 100 pages")
        page_number = 100
        
except Exception as e:
    print(f"❌ Error detecting page count: {e}, defaulting to 100")
    page_number = 100

for i_1 in range(1, page_number + 1):
    try:
        print(f'=========== 📖 Processing page {i_1}/{page_number} ===========')
        
        if i_1 == 1:
            url_1 = 'https://www.ecolex.org/result/?q=&type=legislation&xsubjects=Agricultural+%26+rural+development&xsubjects=Air+%26+atmosphere&xsubjects=Energy&xsubjects=Environment+gen.&xsubjects=Forestry&xsubjects=General&xsubjects=Land+%26+soil&xsubjects=Mineral+resources&xdate_min=1900&xdate_max=2021'
        else:
            url_1 = f'https://www.ecolex.org/result/?type=legislation&xsubjects=Agricultural+%26+rural+development&xsubjects=Air+%26+atmosphere&xsubjects=Energy&xsubjects=Environment+gen.&xsubjects=Forestry&xsubjects=General&xsubjects=Land+%26+soil&xsubjects=Mineral+resources&page={i_1}'
        
        res_1 = get_page(url_1)
        if res_1 is None:
            print(f"⚠️  Skipping page {i_1} due to network errors")
            continue
            
        data_1 = etree.HTML(res_1)
        url2_list = data_1.xpath('//h3[@class="search-result-title"]/a/@href')
        print(f"📊 Found {len(url2_list)} policies on page {i_1}")
        
        if len(url2_list) == 0:
            print("⚠️  No policies found on this page, continuing...")
            continue
        
        # Process policies with multithreading (reduced workers for stability)
        valid_results = []
        with ThreadPoolExecutor(max_workers=5) as pool:
            results = pool.map(parse_detail, url2_list)
            for result in results:
                if result:  # Only include non-None results
                    valid_results.append(result)
        
        # Write results to file in batches for better performance
        if valid_results:
            with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                csv_writer = csv.writer(e)
                for result in valid_results:
                    csv_writer.writerow(result)
            print(f"📝 Wrote {len(valid_results)} policies from page {i_1}")
        
        # Progress report every 10 pages
        if i_1 % 10 == 0:
            print(f"📊 Progress Report - Page {i_1}/{page_number}")
            print(f"   💾 Total saved: {saved_count} policies")
            print(f"   ⏭️  Total skipped: {skipped_count} policies")
            print("=" * 40)

    except Exception as e:
        print(f"❌ Error processing page {i_1}: {e}")
        continue

print(f"\n🎉 ECOLEX Legislation crawling completed!")
print(f"📊 Final Statistics:")
print(f"   💾 Total saved: {saved_count} policies")
print(f"   ⏭️  Total skipped: {skipped_count} policies")
print(f"📂 Output saved to: {output_file}")
