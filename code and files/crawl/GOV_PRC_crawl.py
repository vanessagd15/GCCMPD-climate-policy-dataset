"""
⚠️  NOTE: This crawler does NOT work due to Chinese government restrictions
================================================================================
The Chinese government policy database (xxgk.www.gov.cn) is geo-blocked and 
returns 404 errors for all requests outside China. Access requires either:

1. A VPN with Chinese IP address
2. Physical access from within China
3. Special credentials from the Chinese government

This source cannot be accessed via automated crawling from outside China.
================================================================================
"""

import time
import requests
from fake_useragent import UserAgent
from lxml import etree
import csv
import json
import os

# Create output directory if it doesn't exist
output_dir = os.path.join(os.getcwd(), "data_new")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_file = os.path.join(output_dir, "GOV_PRC.csv")

# Initialize CSV file with headers (matching original approach)
if not os.path.exists(output_file):
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as e:
        csv_writer = csv.writer(e)
        csv_writer.writerow(['Policy', 'Year', 'Country', 'Policy_Content', 'URL', 'Scope', 'Source'])


def get_page(url):
    """Simple page fetcher - exactly like original"""
    ua = UserAgent()
    usr_ag = ua.random
    headers = {'User-Agent': usr_ag}
    response = requests.get(url, headers=headers)
    print(response.status_code)
    response.encoding = 'utf-8'
    response = response.text
    return response


print("🚀 Starting GOV PRC crawler (Original approach)")
print(f"📂 Output: {output_file}")
print("=" * 50)

# Original category list
num_list = ['国土资源、能源%5C矿产', '国土资源、能源%5C煤炭', '国土资源、能源%5C石油与天然气', '国土资源、能源%5C电力']

for num_d in num_list:
    n_k = 1
    print(f'\n爬取序号{num_d}')
    
    while True:
        print(f'爬取页数{n_k}')
        
        # Original domain with simple fallback
        domains = ['http://xxgk.www.gov.cn']
        
        res_1 = None
        for domain in domains:
            try:
                url = f'{domain}/search-zhengce/?callback=jQuery1124017801747997612605_1678622720550&mode=smart&sort=relevant&page_index={n_k}&page_size=10&title=&theme={num_d}&_=1678622720562'
                
                for n_p in range(3):
                    try:
                        raw_response = get_page(url)
                        if raw_response:
                            res_1 = str(raw_response).split('jQuery1124017801747997612605_1678622720550(')[-1][0:-2]
                            break
                    except:
                        continue
                if res_1:
                    break
            except:
                continue
        
        if res_1 is None:
            print('❌ Failed to fetch data')
            break
            
        print(res_1)
        
        # Parse JSON - exactly like original
        try:
            js_data = json.loads(res_1)
            target_list = js_data['data']
        except:
            print('❌ Failed to parse JSON')
            break
        
        n_k += 1
        if len(target_list) < 1:
            break
            
        # Process each policy - exactly like original
        for single_target in target_list:
            try:
                try:
                    Policy = single_target['title']
                except:
                    Policy = ''
                try:
                    Year = str(single_target['writetime']).split('年')[0].replace('\'', '')
                except:
                    Year = ''

                url_2 = single_target['url']
                res_2 = get_page(url_2)
                data_2 = etree.HTML(res_2)
                
                try:
                    Policy_Content_ls_ache = data_2.xpath('//td[@class="b12c"]//text()')[0]
                    Policy_Content_ls = data_2.xpath('//td[@class="b12c"]//text()')
                    Policy_Content = ''
                    for single_Policy_Content in Policy_Content_ls:
                        if single_Policy_Content != '\n':
                            Policy_Content += single_Policy_Content.replace("\n", ' ').strip()
                except:
                    Policy_Content = ''
                    
                if Policy != '':
                    with open(output_file, 'a', encoding='utf-8-sig', newline='') as e:
                        csv_writer = csv.writer(e)
                        csv_writer.writerow([Policy, Year, 'China', Policy_Content, url_2, 'National', 'GOV_CHN'])
                    print([Policy, Year, 'China', Policy_Content[:100], url_2, 'National', 'GOV_CHN'])
                    
                time.sleep(2)  # Original delay
            except:
                time.sleep(2)
                continue

print("🎉 Crawling completed!")
