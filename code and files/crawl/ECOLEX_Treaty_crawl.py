import csv
import time
import os
import random
import re
import requests
from lxml import etree
from fake_useragent import UserAgent

# Configuration: Only extract policies from this year onwards
MIN_YEAR = 2021

# Create output directory if it doesn't exist
output_dir = os.path.join(os.getcwd(), "data_new")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_file = os.path.join(output_dir, "ECOLEX_Treaty.csv")


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


# Check if file exists, if not create with headers
if not os.path.exists(output_file):
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as e:
        csv_writer = csv.writer(e)
        csv_writer.writerow(['Policy', 'Year', 'Country', 'Policy_Content', 'URL', 'Subject', 'Document_Type', 'Keyword', 'Entry into force', 'Source'])

print(f"🚀 Starting ECOLEX Treaty crawler")
print(f"📂 Output file: {output_file}")
print(f"📅 Filtering for policies from {MIN_YEAR} onwards")
print("=" * 60)

# Keep track of progress
saved_count = 0
skipped_count = 0

# Get initial page to determine total pages
print("🔍 Detecting total number of pages...")
initial_url = 'https://www.ecolex.org/result/?type=treaty&xsubjects=Air+%26+atmosphere&xsubjects=Environment+gen.&xsubjects=Land+%26+soil&xsubjects=Mineral+resources&xsubjects=Agricultural+%26+rural+development&xsubjects=Energy&xsubjects=Forestry&xsubjects=General&xdate_max=2021&xdate_min=1900'

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
        print("⚠️  Could not detect pagination, defaulting to 50 pages")
        page_number = 50
        
except Exception as e:
    print(f"❌ Error detecting page count: {e}, defaulting to 50")
    page_number = 50

for i_1 in range(1, page_number + 1):
    try:
        print(f'=========== 📖 Processing page {i_1}/{page_number} ===========')
        
        if i_1 == 1:
            url_1 = 'https://www.ecolex.org/result/?type=treaty&xsubjects=Air+%26+atmosphere&xsubjects=Environment+gen.&xsubjects=Land+%26+soil&xsubjects=Mineral+resources&xsubjects=Agricultural+%26+rural+development&xsubjects=Energy&xsubjects=Forestry&xsubjects=General&xdate_max=2021&xdate_min=1900'
        else:
            url_1 = f'https://www.ecolex.org/result/?type=treaty&xsubjects=Air+%26+atmosphere&xsubjects=Environment+gen.&xsubjects=Land+%26+soil&xsubjects=Mineral+resources&xsubjects=Agricultural+%26+rural+development&xsubjects=Energy&xsubjects=Forestry&xsubjects=General&xdate_max=2021&xdate_min=1900&page={i_1}'
        
        res_1 = get_page(url_1)
        if res_1 is None:
            print(f"⚠️  Skipping page {i_1} due to network errors")
            continue
            
        data_1 = etree.HTML(res_1)
        url2_list = data_1.xpath('//h3[@class="search-result-title"]/a/@href')
        print(f"📊 Found {len(url2_list)} treaties on page {i_1}")
        
        if len(url2_list) == 0:
            print("⚠️  No treaties found on this page, continuing...")
            continue
            
        for treaty_index, single_url2_ache in enumerate(url2_list):
            try:
                print(f'*********** 📑 Treaty {treaty_index + 1}/{len(url2_list)} ************')
                single_url_2 = 'https://www.ecolex.org{}'.format(single_url2_ache)
                print(single_url_2)
                
                res_2 = get_page(single_url_2)
                if res_2 is None:
                    print(f"⚠️  Skipping treaty due to network error")
                    continue
                    
                data_2 = etree.HTML(res_2)
                
                # Extract Policy/Treaty name
                try:
                    Policy = data_2.xpath('//h1/text()')[0].strip()
                except:
                    Policy = ''
                    
                if not Policy:
                    print("⚠️  No treaty name found, skipping...")
                    continue
                    
                print(f"📋 Treaty: {Policy}")
                
                # Extract first section data (Document Type, Date)
                Year = ''
                Document_Type = ''
                Country = ''
                
                try:
                    other_data_list = data_2.xpath('//header/dl')[0]
                    dt_list = other_data_list.xpath('./dt/text()')
                    dd_list = other_data_list.xpath('./dd')
                    
                    for s_1 in range(len(dt_list)):
                        try:
                            if 'Document type' in dt_list[s_1]:
                                doc_type_elements = dd_list[s_1].xpath('./text()')
                                if doc_type_elements:
                                    Document_Type = doc_type_elements[0].strip()
                            elif 'Date' in dt_list[s_1]:
                                date_elements = dd_list[s_1].xpath('./text()')
                                if date_elements:
                                    date_text = str(date_elements[0]).strip()
                                    # Extract year from date text (often format: "Month day, YEAR")
                                    year_match = re.search(r'\b(19|20)\d{2}\b', date_text)
                                    if year_match:
                                        Year = year_match.group()
                                    else:
                                        # Fallback: try to split by comma and get last part
                                        parts = date_text.split(', ')
                                        if len(parts) > 1 and parts[-1].isdigit():
                                            Year = parts[-1]
                        except Exception as e:
                            continue
                except Exception as e:
                    pass
                    
                print(f"📅 Year: {Year}, 📄 Document Type: {Document_Type}")

                # Extract second section data (Subject, Keywords, Entry into force)
                Subject = ''
                Keyword = ''
                Entry_into_force = ''
                
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
                                keyword_elements = dd_list_2[s_2].xpath('.//text()')
                                Keyword = ', '.join([k.strip() for k in keyword_elements if k.strip()])
                            elif 'Entry into force' in dt_list_2[s_2]:
                                entry_elements = dd_list_2[s_2].xpath('./text()')
                                if entry_elements:
                                    Entry_into_force = entry_elements[0].strip()
                        except Exception as e:
                            continue
                except Exception as e:
                    pass
                    
                print(f"🏷️  Subject: {Subject}, 🔑 Keywords: {Keyword}")

                # Extract Policy Content/Abstract
                Policy_Content = ''
                try:
                    abstract_elements = data_2.xpath('//p[@class="abstract"]/text()')
                    if abstract_elements:
                        Policy_Content = abstract_elements[0].strip()
                    else:
                        comment_elements = data_2.xpath('//p[@class="comment"]/text()')
                        if comment_elements:
                            Policy_Content = comment_elements[0].strip()
                except Exception as e:
                    pass

                # Extract Country information
                Country_txt = ''
                try:
                    country_elements = data_2.xpath('//tbody[@class="body"]/tr/th/text()')
                    if country_elements:
                        Country_txt = ', '.join([c.strip() for c in country_elements if c.strip()])
                except Exception as e:
                    pass

                # Apply year filter and save
                try:
                    year_int = int(Year) if Year.isdigit() else 0
                    if year_int >= MIN_YEAR or Year == '':  # Include treaties with no year specified
                        with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                            csv_writer = csv.writer(e)
                            csv_writer.writerow([Policy, Year, Country_txt, Policy_Content, single_url_2, Subject, Document_Type, Keyword, Entry_into_force, 'ECOLEX_Treaty'])
                        saved_count += 1
                        print(f"✅ Saved: {Policy} ({Year})")
                    else:
                        skipped_count += 1
                        print(f"❌ Skipped (before {MIN_YEAR}): {Policy} ({Year})")
                except ValueError:
                    # If year is not a valid integer, save it anyway
                    with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                        csv_writer = csv.writer(e)
                        csv_writer.writerow([Policy, Year, Country_txt, Policy_Content, single_url_2, Subject, Document_Type, Keyword, Entry_into_force, 'ECOLEX_Treaty'])
                    saved_count += 1
                    print(f"⚠️  Saved (non-numeric year): {Policy} ({Year})")
                    
            except Exception as e:
                print(f"❌ Error processing treaty {treaty_index + 1}: {e}")
                continue
        
        # Progress report every 5 pages
        if i_1 % 5 == 0:
            print(f"📊 Progress Report - Page {i_1}/{page_number}")
            print(f"   💾 Saved: {saved_count} treaties")
            print(f"   ⏭️  Skipped: {skipped_count} treaties")
            print("=" * 40)
            
    except Exception as e:
        print(f"❌ Error processing page {i_1}: {e}")
        continue

print(f"\n🎉 ECOLEX Treaty crawling completed!")
print(f"📊 Final Statistics:")
print(f"   💾 Total saved: {saved_count} treaties")
print(f"   ⏭️  Total skipped: {skipped_count} treaties")
print(f"📂 Output saved to: {output_file}")
