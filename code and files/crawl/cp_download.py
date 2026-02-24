"""
⚠️  NOTE: This automated download script does NOT work reliably
================================================================================
The Climate Policy Database website requires user interaction (permission dialogs)
that cannot be automated with Selenium. 

✅ RECOMMENDED: Go directly to https://climatepolicytracker.org/
   and manually download the CSV file. It's faster and more reliable.

This script is kept for reference only.
================================================================================
"""

from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException, NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import random
import time
import os
from pathlib import Path
import shutil

# Configuration
MIN_YEAR = 2021
MAX_RETRIES = 3
RETRY_DELAY = 5
DOWNLOAD_TIMEOUT = 300  # 5 minutes timeout for download

# Create output directory
output_dir = Path('../../data_new')
output_dir.mkdir(exist_ok=True)

# Global counters for tracking
download_count = 0
error_count = 0

# Updated user agents (Chrome only for consistency)
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
]

def setup_chrome_driver():
    """Setup Chrome driver with enhanced options and error handling"""
    try:
        print("🔧 Setting up Chrome WebDriver...")
        
        chrome_options = Options()
        
        # Select Chrome user agent only (for consistency with Chrome WebDriver)
        selected_user_agent = random.choice(USER_AGENTS)
        print(f"🎭 Using Chrome User-Agent: {selected_user_agent}")
        
        # Enhanced Chrome options for stability and performance
        chrome_options.add_argument(f'--user-agent={selected_user_agent}')
        chrome_options.add_argument('--blink-settings=imagesEnabled=false')  # Disable images for speed
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-plugins')
        chrome_options.add_argument('--disable-background-timer-throttling')
        chrome_options.add_argument('--disable-backgrounding-occluded-windows')
        chrome_options.add_argument('--disable-renderer-backgrounding')
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--disable-popup-blocking')
        chrome_options.add_argument('--disable-translate')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--headless')  # Run headless to avoid permission dialogs
        
        # Set download directory to our output folder
        download_dir = str(output_dir.absolute())
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "safebrowsing.disable_download_protection": True,
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.automatic_downloads": 1,
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
            "download.extensions_to_open": "",
            "download.open_pdf_in_system_reader": False
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        print(f"📂 Download directory set to: {download_dir}")
        
        # Initialize driver
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(60)  # 60 second page load timeout
        
        print("✅ Chrome WebDriver initialized successfully")
        return driver
        
    except Exception as e:
        print(f"❌ Error setting up Chrome driver: {e}")
        return None


def wait_for_download_completion(download_dir, timeout=DOWNLOAD_TIMEOUT):
    """Wait for file download to complete"""
    print(f"⏳ Waiting for download completion (timeout: {timeout}s)...")
    
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        # Check for any .crdownload files (Chrome partial downloads)
        crdownload_files = list(Path(download_dir).glob("*.crdownload"))
        
        if not crdownload_files:
            # Check if any CSV files were created recently
            csv_files = list(Path(download_dir).glob("*.csv"))
            if csv_files:
                # Find the most recent CSV file
                latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
                file_age = time.time() - latest_file.stat().st_mtime
                
                if file_age < 60:  # File created within last minute
                    print(f"✅ Download completed: {latest_file.name}")
                    return latest_file
        
        time.sleep(2)
    
    print(f"⏰ Download timeout after {timeout} seconds")
    return None


def download_climate_policy_data():
    """Main function to download climate policy data with robust error handling"""
    global download_count, error_count
    
    # First try: Direct CSV download attempts
    direct_urls = [
        'https://climatepolicydatabase.org/policies/export.csv',
        'https://climatepolicydatabase.org/policies/export?_format=csv',
        'https://climatepolicydatabase.org/policies.csv',
        'https://climatepolicydatabase.org/export/all?format=csv'
    ]
    
    chrome_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    
    for direct_url in direct_urls:
        try:
            print(f"🔄 Attempting direct download: {direct_url}")
            import requests
            
            headers = {
                'User-Agent': chrome_user_agent,
                'Accept': 'text/csv,application/csv,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            response = requests.get(direct_url, headers=headers, stream=True, timeout=60)
            print(f"📡 Response status: {response.status_code}")
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                content_length = response.headers.get('content-length', 'unknown')
                print(f"📄 Content-Type: {content_type}")
                print(f"📊 Content-Length: {content_length}")
                
                if 'csv' in content_type or 'text' in content_type or content_length != 'unknown':
                    filename = f"climate_policy_database_{time.strftime('%Y%m%d_%H%M%S')}.csv"
                    filepath = output_dir / filename
                    
                    print(f"💾 Downloading to: {filepath}")
                    with open(filepath, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    
                    if filepath.exists() and filepath.stat().st_size > 0:
                        # Validate that we actually got CSV data, not HTML
                        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                            first_lines = f.read(1000).strip()
                        
                        if first_lines.startswith('<!DOCTYPE html>') or first_lines.startswith('<html'):
                            print("❌ Downloaded HTML page instead of CSV data")
                            filepath.unlink()  # Delete the HTML file
                        elif 'policy_id' in first_lines.lower() or 'country' in first_lines.lower() or first_lines.count(',') > 5:
                            file_size = filepath.stat().st_size
                            print(f"✅ Direct download successful: {filename} ({file_size:,} bytes)")
                            download_count += 1
                            return filepath
                        else:
                            print(f"❌ Downloaded content doesn't look like policy CSV data")
                            print(f"📄 First 200 chars: {first_lines[:200]}")
                            filepath.unlink()  # Delete the invalid file
                    else:
                        print("❌ Downloaded file is empty")
                else:
                    print(f"❌ Unexpected content type: {content_type}")
            else:
                print(f"❌ HTTP error: {response.status_code}")
        
        except Exception as e:
            print(f"❌ Direct download failed for {direct_url}: {e}")
    
    print("🔄 All direct download attempts failed, trying browser automation...")
    
    # Fallback to Selenium approach
    webpage_urls = [
        'https://climatepolicydatabase.org/policies/export?page&_format=csv',
        'https://climatepolicydatabase.org/policies',
        'https://climatepolicydatabase.org/policies/export'
    ]
    
    for attempt in range(MAX_RETRIES):
        driver = None
        
        for url in webpage_urls:
            try:
                print(f"\n🚀 Attempt {attempt + 1}/{MAX_RETRIES} for URL: {url}")
                
                # Setup Chrome driver
                driver = setup_chrome_driver()
                if driver is None:
                    print("❌ Failed to initialize Chrome driver")
                    continue
                
                # Navigate to the page
                print("🌐 Navigating to climate policy database...")
                driver.get(url)
                
                # Wait for page to load
                print("⏳ Waiting for page to load...")
                time.sleep(5)
            
            # Try multiple selectors for the download button
            download_selectors = [
                (By.ID, "vde-automatic-download"),
                (By.CLASS_NAME, "btn-download"),
                (By.XPATH, "//a[contains(@href, 'export')]"),
                (By.XPATH, "//button[contains(text(), 'Download')]"),
                (By.XPATH, "//a[contains(text(), 'Download')]"),
                (By.XPATH, "//input[@type='submit']"),
                (By.CSS_SELECTOR, "[data-drupal-selector='edit-submit']"),
                (By.CSS_SELECTOR, "input[value*='Download']"),
            ]
            
            button = None
            for selector_type, selector_value in download_selectors:
                try:
                    print(f"🔍 Trying selector: {selector_type} = '{selector_value}'")
                    button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((selector_type, selector_value))
                    )
                    print(f"✅ Found download button with {selector_type}")
                    break
                except Exception as e:
                    print(f"❌ Selector failed: {e}")
                    continue
            
            if button is None:
                # Try to find any clickable element that might be the download
                print("🔍 Searching for any download-related elements...")
                try:
                    # Print page title for debugging
                    print(f"📄 Page title: {driver.title}")
                    
                    # Look for common download patterns
                    buttons = driver.find_elements(By.TAG_NAME, "button")
                    links = driver.find_elements(By.TAG_NAME, "a")
                    inputs = driver.find_elements(By.TAG_NAME, "input")
                    
                    print(f"🔢 Found {len(buttons)} buttons, {len(links)} links, {len(inputs)} inputs")
                    
                    # Debug: print some button/link text
                    print("🔍 Available buttons:")
                    for i, btn in enumerate(buttons[:5]):  # Show first 5
                        try:
                            text = btn.text or btn.get_attribute('value') or btn.get_attribute('title') or 'No text'
                            print(f"   Button {i+1}: {text[:100]}")
                        except:
                            pass
                    
                    print("🔍 Available links:")
                    for i, link in enumerate(links[:5]):  # Show first 5
                        try:
                            text = link.text or link.get_attribute('title') or link.get_attribute('href') or 'No text'
                            print(f"   Link {i+1}: {text[:100]}")
                        except:
                            pass
                    
                    all_elements = buttons + links + inputs
                    for elem in all_elements:
                        try:
                            text = elem.get_attribute('value') or elem.text or elem.get_attribute('title') or ''
                            href = elem.get_attribute('href') or ''
                            if any(keyword in text.lower() for keyword in ['download', 'export', 'csv']) or 'export' in href.lower():
                                button = elem
                                print(f"✅ Found potential download element: {text[:50]}")
                                break
                        except:
                            continue
                except Exception as e:
                    print(f"❌ Error searching for elements: {e}")
            
            if button is None:
                # Save page source for debugging
                try:
                    page_source_path = output_dir / "debug_page_source.html"
                    with open(page_source_path, 'w', encoding='utf-8') as f:
                        f.write(driver.page_source)
                    print(f"🐛 Page source saved to: {page_source_path}")
                    
                    # Also try to take a screenshot
                    screenshot_path = output_dir / "debug_screenshot.png"
                    driver.save_screenshot(str(screenshot_path))
                    print(f"📸 Debug screenshot saved to: {screenshot_path}")
                except Exception as e:
                    print(f"⚠️ Could not save debug files: {e}")
                
                raise Exception("Could not find download button with any known selector")
            
            # Click the download button
            print("🖱️  Clicking download button...")
            try:
                driver.execute_script("arguments[0].click();", button)
            except:
                # Fallback to regular click
                button.click()
            
            print("📥 Download initiated, waiting for completion...")
            
            # Handle potential download permission dialog
            try:
                print("🔍 Checking for download permission dialog...")
                # Wait a moment for any dialogs to appear
                time.sleep(5)
                
                # Try to accept any download permission dialogs
                # Look for common dialog buttons
                dialog_buttons = driver.find_elements(By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'allow')] | //button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')] | //button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'continue')]")
                
                for dialog_btn in dialog_buttons:
                    try:
                        if dialog_btn.is_displayed() and dialog_btn.is_enabled():
                            print("✅ Found and clicking permission dialog button...")
                            dialog_btn.click()
                            time.sleep(2)
                            break
                    except:
                        continue
                        
            except Exception as e:
                print(f"ℹ️  No permission dialog handling needed: {e}")
            
                
                try:
                    downloaded_file.rename(final_path)
                    print(f"📁 File renamed to: {new_filename}")
                except Exception as e:
                    print(f"⚠️  Could not rename file: {e}")
                    final_path = downloaded_file
                
                # Verify file content
                if final_path.exists() and final_path.stat().st_size > 0:
                    file_size = final_path.stat().st_size
                    print(f"✅ Download successful: {final_path.name} ({file_size:,} bytes)")
                    download_count += 1
                    return final_path
                else:
                    raise Exception("Downloaded file is empty or missing")
            else:
                raise Exception("Download did not complete within timeout")
                
        except TimeoutException:
            print(f"⏰ Timeout on attempt {attempt + 1}: Page or element took too long to load")
            error_count += 1
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        except NoSuchElementException:
            print(f"🔍 Element not found on attempt {attempt + 1}: Download button missing")
            error_count += 1
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        except WebDriverException as e:
            print(f"🚗 WebDriver error on attempt {attempt + 1}: {e}")
            error_count += 1
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        except Exception as e:
            print(f"❌ Unexpected error on attempt {attempt + 1}: {e}")
            error_count += 1
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        finally:
            # Always close the driver
            if driver:
                try:
                    driver.quit()
                    print("🔒 Chrome driver closed")
                except:
                    pass
    
    print(f"❌ Failed to download after {MAX_RETRIES} attempts")
    return None


def main():
    """Main execution function with comprehensive error handling"""
    print("=" * 60)
    print("🌍 Climate Policy Database Downloader")
    print("🎯 Enhanced version with robust error handling and progress tracking")
    print("=" * 60)
    print(f"📂 Output directory: {output_dir.absolute()}")
    print(f"📅 Note: Complete dataset downloaded (apply year filter >= {MIN_YEAR} during analysis if needed)")
    
    try:
        result = download_climate_policy_data()
        
        if result:
            print(f"\n🎉 Climate policy data download completed!")
            print(f"📊 Final Statistics:")
            print(f"   ✅ Files downloaded: {download_count}")
            print(f"   ❌ Total errors: {error_count}")
            print(f"📂 Output file: {result}")
            
            print(f"\n💡 Next steps:")
            print(f"   1. Review downloaded CSV file for data quality")
            print(f"   2. Apply year filtering (>= {MIN_YEAR}) during analysis")
            print(f"   3. Integrate with main climate policy database")
        else:
            print(f"\n❌ Download failed completely")
            print(f"📊 Error Statistics:")
            print(f"   ❌ Total errors: {error_count}")
            
    except Exception as e:
        print(f"❌ Critical error: {e}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Download interrupted by user")
        print(f"📊 Progress so far:")
        print(f"   ✅ Downloaded: {download_count} files")
        print(f"   ❌ Errors: {error_count}")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        print(f"📊 Progress before error:")
        print(f"   ✅ Downloaded: {download_count} files")
        print(f"   ❌ Errors: {error_count}")
