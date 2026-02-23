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
output_dir = Path('../data_new')
output_dir.mkdir(exist_ok=True)

# Global counters for tracking
download_count = 0
error_count = 0

# Updated user agents (more recent versions)
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0'
]

def setup_chrome_driver():
    """Setup Chrome driver with enhanced options and error handling"""
    try:
        print("🔧 Setting up Chrome WebDriver...")
        
        chrome_options = Options()
        
        # Select random user agent
        selected_user_agent = random.choice(USER_AGENTS)
        print(f"🎭 Using User-Agent: {selected_user_agent}")
        
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
        chrome_options.add_argument('--window-size=1920,1080')
        
        # Set download directory to our output folder
        download_dir = str(output_dir.absolute())
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
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
    
    url = 'https://climatepolicydatabase.org/policies/export?page&_format=csv'
    
    for attempt in range(MAX_RETRIES):
        driver = None
        try:
            print(f"\n🚀 Attempt {attempt + 1}/{MAX_RETRIES} to download climate policy data")
            print(f"🔗 Target URL: {url}")
            
            # Setup Chrome driver
            driver = setup_chrome_driver()
            if driver is None:
                raise Exception("Failed to initialize Chrome driver")
            
            # Navigate to the page
            print("🌐 Navigating to climate policy database...")
            driver.get(url)
            
            # Wait for the download button to be present
            print("⏳ Waiting for download button to appear...")
            element = WebDriverWait(driver, 100).until(
                EC.presence_of_element_located((By.ID, "vde-automatic-download"))
            )
            
            # Find and click the download button
            print("🖱️  Clicking download button...")
            button = driver.find_element(By.ID, "vde-automatic-download")
            driver.execute_script("arguments[0].click();", button)
            
            print("📥 Download initiated, waiting for completion...")
            
            # Wait for download to complete
            downloaded_file = wait_for_download_completion(output_dir)
            
            if downloaded_file:
                # Rename file to standardized format
                new_filename = f"climate_policy_database_{time.strftime('%Y%m%d_%H%M%S')}.csv"
                final_path = output_dir / new_filename
                
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
    print(f"📅 Data processing note: Apply year filter (>= {MIN_YEAR}) during analysis")
    
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
