import requests
from lxml import html
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import pandas as pd
# from config import iea, policy, all_policy  # Commented out - may not be available
import re
import os
import time
import shutil
import random
import csv

# Optional Excel support - handle gracefully if not available
try:
    import openpyxl
    from openpyxl import Workbook
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    EXCEL_AVAILABLE = True
except ImportError:
    print("ℹ️  Excel support not available - using CSV output only")
    EXCEL_AVAILABLE = False

# Optional MongoDB imports - handle gracefully if not available
try:
    from pymongo import MongoClient
    from pymongo.errors import DuplicateKeyError
    MONGODB_AVAILABLE = True
except ImportError:
    print("ℹ️  MongoDB not available - crawler will work without database storage")
    MongoClient = None
    DuplicateKeyError = Exception
    MONGODB_AVAILABLE = False

# Configuration: Only extract policies from this year onwards
MIN_YEAR = 2021

# Create output directory if it doesn't exist
output_dir = os.path.join(os.getcwd(), "data_new")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_file = os.path.join(output_dir, "IEA_all_policy.csv")

print(f"🚀 Starting IEA All Policy crawler")
print(f"📂 Output file: {output_file}")
print(f"📅 Filtering for policies from {MIN_YEAR} onwards")
print(f"🏛️  Source: International Energy Agency (IEA)")
print("=" * 60)

# Keep track of progress
saved_count = 0
skipped_count = 0


class PoliciesSpider(object):
    def __init__(self):
        self.start_url = "https://www.iea.org/policies"
        self.headers = {
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
        }
        # Optional MongoDB connection - handle gracefully if not available
        try:
            if MONGODB_AVAILABLE:
                self.client = MongoClient()
                self.collection = self.client['IEA']['all_policy']
                self.use_mongodb = True
                print("✅ MongoDB connection established")
            else:
                self.use_mongodb = False
                print("ℹ️  Running without MongoDB - data will be saved to CSV only")
        except Exception as e:
            print(f"⚠️  MongoDB not available: {e}")
            self.use_mongodb = False
        
        global saved_count, skipped_count

    def md5Encode(self, str):
        m = hashlib.md5()
        m.update(str)
        return m.hexdigest()

    def get_url_list(self, url, page):
        new_url_list = [url + "?page=%d" % i for i in range(1, page + 1)]
        return new_url_list

    def parse(self, url, max_retries=3):
        """Get page content with retry logic and better error handling"""
        for attempt in range(max_retries):
            try:
                # Add random delay between requests (1-3 seconds)
                time.sleep(random.uniform(1, 3))
                
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()  # Raise exception for bad status codes
                print(f"✅ Status {response.status_code} for {url}")
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

    def get_content_list(self, rest):
        global saved_count, skipped_count
        
        if rest is None:
            print("❌ No content to process")
            return
            
        try:
            html_rest = html.etree.HTML(rest)
            # Get total count
            total_elements = html_rest.xpath('//span[contains(@class,"m-filter-bar__count")]/text()')
            if not total_elements:
                print("❌ Could not find total count")
                return
                
            total = int(total_elements[0])
            page = total // 30 + 1 if total % 30 > 0 else total // 30
            print(f"📊 Found {total} total policies across {page} pages")
            
            url_list = self.get_url_list(self.start_url, page)
            item_list = []
            list_url = []
            
            # Create CSV file with headers
            if not os.path.exists(output_file):
                with open(output_file, 'w', encoding='utf-8-sig', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['Policy', 'Country', 'Year', 'Status', 'Jurisdiction', 'policy_url', 'Topics', 'Type', 'Sectors', 'Technologies', 'LearnMore', 'Policy_Content', 'Source'])
            
            print("🔍 Extracting policy list from all pages...")
            for page_index, page_url in enumerate(url_list):
                print(f"📖 Processing page {page_index + 1}/{len(url_list)}")
                
                rest_list = self.parse(page_url)
                if rest_list is None:
                    print(f"⚠️  Skipping page {page_index + 1} due to network error")
                    continue
                    
                try:
                    rest_list_html = html.etree.HTML(rest_list)
                    li_list = rest_list_html.xpath('//ul[@class="m-policy-listing-items"]/li')
                    
                    for li in li_list:
                        try:
                            new_item = dict()
                            new_item['Policy'] = li.xpath('.//a[@class="m-policy-listing-item__link"]/text()')[0].replace('\n', "").strip()
                            new_item['Country'] = li.xpath('./div[@class="m-policy-listing-item-row__content"]/span[1]/text()')[0].replace('\n', "").strip()
                            new_item['Year'] = li.xpath('./div[@class="m-policy-listing-item-row__content"]/span[2]/text()')[0].replace('\n', "").strip()
                            new_item['Status'] = li.xpath('./div[@class="m-policy-listing-item-row__content"]/span[3]/text()')[0].replace('\n', "").strip()
                            new_item['Jurisdiction'] = li.xpath('./div[@class="m-policy-listing-item-row__content"]/span[4]/text()')[0].replace('\n', "").strip()
                            new_item['policy_url'] = "https://www.iea.org" + li.xpath('.//a[@class="m-policy-listing-item__link"]/@href')[0]
                            
                            # Apply year filter here for efficiency
                            try:
                                year_int = int(new_item['Year']) if new_item['Year'].isdigit() else 0
                                if year_int < MIN_YEAR and new_item['Year'] != '':
                                    skipped_count += 1
                                    print(f"❌ Skipped (before {MIN_YEAR}): {new_item['Policy']} ({new_item['Year']})")
                                    continue
                            except ValueError:
                                pass  # Include non-numeric years
                            
                            print(f"📋 Found: {new_item['Policy']} ({new_item['Country']}, {new_item['Year']})")
                            item_list.append(new_item)
                            list_url.append(new_item['policy_url'])
                            
                        except Exception as e:
                            print(f"⚠️  Error parsing policy item: {e}")
                            continue
                            
                except Exception as e:
                    print(f"❌ Error parsing page {page_index + 1}: {e}")
                    continue
            
            print(f"🎯 Processing {len(item_list)} policies in detail...")
            
            # Process detailed policy information with multithreading (reduced workers for stability)
            with ThreadPoolExecutor(max_workers=3) as pool:
                results = pool.map(self.parse_detail, list_url, item_list)
                
                # Clear MongoDB collection if available
                if self.use_mongodb:
                    try:
                        self.collection.drop()
                        print("🗑️  Cleared MongoDB collection")
                    except Exception as e:
                        print(f"⚠️  Could not clear MongoDB: {e}")
                
                # Process results and save to CSV
                for result in results:
                    if result:
                        try:
                            # Write to CSV
                            with open(output_file, 'a', encoding='utf-8-sig', newline='') as csvfile:
                                writer = csv.writer(csvfile)
                                writer.writerow([
                                    result.get('Policy', ''),
                                    result.get('Country', ''),
                                    result.get('Year', ''),
                                    result.get('Status', ''),
                                    result.get('Jurisdiction', ''),
                                    result.get('policy_url', ''),
                                    result.get('Topics', ''),
                                    result.get('Policy type', ''),
                                    result.get('Sectors', ''),
                                    result.get('Technologies', ''),
                                    result.get('LearnMore', ''),
                                    result.get('Policy_Content', ''),
                                    'IEA'
                                ])
                            
                            saved_count += 1
                            print(f"✅ Saved: {result.get('Policy', 'Unknown')} ({result.get('Year', 'Unknown')})")
                            
                            # Save to MongoDB if available
                            if self.use_mongodb:
                                self.save(result)
                                
                        except Exception as e:
                            print(f"❌ Error saving result: {e}")
                            continue
                            
        except Exception as e:
            print(f"❌ Critical error in content processing: {e}")

        print(f"\n🎉 IEA policy extraction completed!")
        print(f"📊 Final Statistics:")
        print(f"   💾 Total saved: {saved_count} policies")
        print(f"   ⏭️  Total skipped: {skipped_count} policies")
        print(f"📂 Output saved to: {output_file}")

    #         df = pd.DataFrame(data)
    #         print(df)
    #         # 指定生成的Excel表格名称
    #         file_path = pd.ExcelWriter('All_Policies.xlsx')
    #         # 替换空单元格
    #         df.fillna(' ', inplace=True)
    #         # 输出
    #         df.to_excel(file_path, encoding='utf-8', index=False, engine='xlsxwriter')
    #         # 保存表格
    #         file_path.save()

    def parse_detail(self, url, item):
        """Parse detailed policy information with robust error handling"""
        try:
            print(f"🔍 Processing details for: {item.get('Policy', 'Unknown')}")
            
            rest = self.parse(url)
            if rest is None:
                print(f"❌ Failed to fetch details for: {item.get('Policy', 'Unknown')}")
                return None
                
            html_rest = html.etree.HTML(rest)
            
            try:
                # Extract Topics
                topics_list = html_rest.xpath('//span[contains(text(),"Topics")]/following-sibling::ul/li/a/span[1]/text()')
                topics_list = [i.strip().replace('\n', '').replace('\xa0', '') for i in topics_list]
                topics_list_temp = set(";".join(set(topics_list)).split(';'))
                topics_list_temp.discard("")
                item['Topics'] = ";".join(topics_list_temp)

                # Extract Policy types
                policy_type_list = html_rest.xpath('//span[contains(text(),"Policy types")]/following-sibling::ul/li/a/span[1]/text()')
                policy_type_list = [i.strip().replace('\n', '').replace('\xa0', '') for i in policy_type_list]
                policy_type_list_temp = set(";".join(set(policy_type_list)).split(';'))
                policy_type_list_temp.discard("")
                item['Policy type'] = ";".join(policy_type_list_temp)

                # Extract Sectors
                sectors_list = html_rest.xpath('//span[contains(text(),"Sectors")]/following-sibling::ul/li/a/span[1]/text()')
                sectors_list = [i.strip().replace('\n', '').replace('\xa0', '') for i in sectors_list]
                sectors_list_temp = set(";".join(set(sectors_list)).split(';'))
                sectors_list_temp.discard("")
                item['Sectors'] = ";".join(sectors_list_temp)

                # Extract Technologies
                technologies_list = html_rest.xpath('//span[contains(text(),"Technologies")]/following-sibling::ul/li/a/span[1]/text()')
                technologies_list = [i.strip().replace('\n', '').replace('\xa0', '') for i in technologies_list]
                technologies_list_temp = set(";".join(set(technologies_list)).split(';'))
                technologies_list_temp.discard("")
                item['Technologies'] = ";".join(technologies_list_temp)

                # Extract Learn more URL
                learn_more = html_rest.xpath('//span[contains(text(),"Learn more")]/../@href')
                item['LearnMore'] = learn_more[0] if len(learn_more) else ''

                # Extract Policy content with multiple fallbacks
                content = html_rest.xpath('//div[contains(@class,"m-block__content")]/p[not(position()=last())]/text()')
                if len(content) > 0 and content[0].strip():
                    item['Policy_Content'] = '\n'.join(content).replace('\n', '').replace('\xa0', '').strip()
                else:
                    content = html_rest.xpath('//div[contains(@class,"m-block__content")]/text()')
                    if len(content) > 0 and content[0].strip():
                        item['Policy_Content'] = '\n'.join(content).replace('\n', '').replace('\xa0', '').strip()
                    else:
                        content = html_rest.xpath('//div[contains(@class,"m-block__content")]//font/text()')
                        item['Policy_Content'] = '\n'.join(content).replace('\n', '').replace('\xa0', '').strip() if len(content) > 0 else ""

                # Limit content length for CSV compatibility
                if len(item['Policy_Content']) > 5000:
                    item['Policy_Content'] = item['Policy_Content'][:5000] + '...'

                # Generate unique ID
                item['_id'] = self.md5Encode((item["Policy"] + item["Country"] + item["Year"] + item["Policy_Content"]).encode('utf-8'))
                
                # Clean all string fields for CSV compatibility
                for key, value in item.items():
                    if isinstance(value, str):
                        # Remove problematic characters for CSV
                        item[key] = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                        item[key] = ' '.join(item[key].split())  # Remove extra whitespace

                print(f"✅ Successfully processed: {item.get('Policy', 'Unknown')}")
                return item
                
            except Exception as e:
                print(f"⚠️  Error extracting details for {item.get('Policy', 'Unknown')}: {e}")
                # Return basic item even if details extraction fails
                item['Topics'] = ""
                item['Policy type'] = ""
                item['Sectors'] = ""
                item['Technologies'] = ""
                item['LearnMore'] = ""
                item['Policy_Content'] = ""
                item['_id'] = f"IEA_{item.get('Country', 'Unknown')}_{item.get('Year', 'Unknown')}_{len(item.get('Policy', ''))}"
                return item
                
        except Exception as e:
            print(f"❌ Critical error processing {url}: {e}")
            return None

    def save(self, item):
        """Save item to MongoDB if available"""
        if self.use_mongodb and MONGODB_AVAILABLE and self.collection:
            try:
                self.collection.insert_one(item)
                print(f"💾 Saved to MongoDB: {item.get('Policy', 'Unknown')}")
            except DuplicateKeyError:
                print(f"⚠️  Duplicate entry in MongoDB: {item.get('Policy', 'Unknown')}")
            except Exception as e:
                print(f"❌ Error saving to MongoDB: {e}")
        else:
            # MongoDB not available - data already saved to CSV
            pass

    def run(self):
        """Main execution method with comprehensive error handling"""
        print("🚀 Starting IEA policy crawler...")
        print(f"📅 Filtering policies from {MIN_YEAR} onwards")
        print(f"📂 Output will be saved to: {output_file}")
        
        try:
            rest = self.parse(self.start_url)
            if rest is None:
                print("❌ Failed to fetch main page. Please check your internet connection and try again.")
                return
                
            self.get_content_list(rest)
            
        except KeyboardInterrupt:
            print("\n⚠️  Crawling interrupted by user")
            print(f"📊 Progress so far:")
            print(f"   💾 Total saved: {saved_count} policies") 
            print(f"   ⏭️  Total skipped: {skipped_count} policies")
        except Exception as e:
            print(f"❌ Critical error during crawling: {e}")
            print(f"📊 Progress before error:")
            print(f"   💾 Total saved: {saved_count} policies")
            print(f"   ⏭️  Total skipped: {skipped_count} policies")


if __name__ == '__main__':
    print("=" * 60)
    print("🌍 IEA Climate Policy Database Crawler")
    print("🎯 Enhanced version with year filtering and robust error handling")
    print("=" * 60)
    
    ps = PoliciesSpider()
    ps.run()
