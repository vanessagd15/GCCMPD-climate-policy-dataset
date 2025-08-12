import csv
import requests
from lxml import etree
from fake_useragent import UserAgent
import os

# Configuration: Only extract policies from this year onwards
MIN_YEAR = 2021


def get_page(url):
    ua = UserAgent()
    usr_ag = ua.random
    headers = {'User-Agent': usr_ag}
    response = requests.get(url, headers=headers, timeout=1.5)
    print(response.status_code)
    response.encoding = 'utf-8'
    response = response.text
    # print(response)
    return response


# Create output directory if it doesn't exist
output_dir = os.path.join(os.getcwd(), "data_new")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_file = os.path.join(output_dir, "APEP_test.csv")
e = open(output_file, 'w', encoding='utf-8-sig', newline='')  # Changed to 'w' to overwrite
csv_writer = csv.writer(e)
csv_writer.writerow(
    ['Policy', 'Year', 'Country', 'Policy_Content', 'single_url_2', 'Scope', 'Document_Type', 'Economic_Sector',
     'Energy_Types', 'Source'])
e.close()

# Test with only first 2 pages
for i_1 in range(2):  # Changed from 218 to 2 for testing
    print(f'=========爬取至{i_1}页===========')
    if i_1 == 0:
        url_1 = 'https://policy.asiapacificenergy.org/node'
    else:
        url_1 = 'https://policy.asiapacificenergy.org/node?page={}'.format(i_1)
    
    try:
        res_1 = get_page(url_1)
        data_1 = etree.HTML(res_1)
        url2_list = data_1.xpath('//a[@rel="tag"]/@href')
        print(f"Found {len(url2_list)} policy links on page {i_1}")
        
        for single_url2_ache in url2_list:
            try:
                print('***********爬取至{}条************'.format(url2_list.index(single_url2_ache)))
                single_url_2 = 'https://policy.asiapacificenergy.org{}'.format(single_url2_ache)
                print(single_url_2)
                res_2 = get_page(single_url_2)
                data_2 = etree.HTML(res_2)
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
                print(f"Policy: {Policy}")
                
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
                        # Effective Start Year
                        if 'Effective Start Year:' in head_text:
                            try:
                                Year = single_other.xpath('./div[2]/div/text()')[0].replace("\n", ' ').strip()
                            except:
                                Year = ''
                        # Scope
                        elif 'Scope:' in head_text:
                            try:
                                Scope = single_other.xpath('./div[2]/div/text()')[0].replace("\n", ' ').strip()
                            except:
                                Scope = ''
                        # Document Type:
                        elif 'Document Type:' in head_text:
                            try:
                                Document_Type = single_other.xpath('./div[2]/div/text()')[0].replace("\n", ' ').strip()
                            except:
                                Document_Type = ''
                        # Economic_Sector
                        elif 'Economic Sector:' in head_text:
                            try:
                                Economic_Sector = single_other.xpath('./div[2]/div/text()')[0].replace("\n", ' ').strip()
                            except:
                                Economic_Sector = ''
                        # Energy_Types
                        elif 'Energy Types:' in head_text:
                            try:
                                Energy_Types = single_other.xpath('./div[2]/div/text()')[0].replace("\n", ' ').strip()
                            except:
                                Energy_Types = ''

                        # Policy_Content
                        elif 'Overall Summary:' in head_text:
                            try:
                                Policy_Content = single_other.xpath('./div[2]/div/text()')[0].replace("\n", ' ').strip()
                            except:
                                Policy_Content = ''
                    except:
                        continue
                        
                if Policy != '':
                    # Filter for policies from MIN_YEAR onwards
                    try:
                        year_int = int(Year) if Year.isdigit() else 0
                        if year_int >= MIN_YEAR or Year == '':  # Include policies with no year specified
                            e = open(output_file, 'a+', encoding='utf-8-sig', newline='')
                            csv_writer = csv.writer(e)
                            csv_writer.writerow(
                                [Policy, Year, Country, Policy_Content, single_url_2, Scope, Document_Type, Economic_Sector,
                                 Energy_Types, 'APEP'])
                            e.close()
                            print(f"✅ Saved: {Policy} ({Year})")
                        else:
                            print(f"❌ Skipped (before {MIN_YEAR}): {Policy} ({Year})")
                    except ValueError:
                        # If year is not a valid integer, save it anyway
                        e = open(output_file, 'a+', encoding='utf-8-sig', newline='')
                        csv_writer = csv.writer(e)
                        csv_writer.writerow(
                            [Policy, Year, Country, Policy_Content, single_url_2, Scope, Document_Type, Economic_Sector,
                             Energy_Types, 'APEP'])
                        e.close()
                        print(f"⚠️  Saved (non-numeric year): {Policy} ({Year})")
            except Exception as e:
                print(f"Error processing policy: {e}")
                continue
    except Exception as e:
        print(f"Error processing page {i_1}: {e}")
        continue

print("Test crawling completed!")
