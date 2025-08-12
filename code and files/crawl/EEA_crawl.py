import requests
import os
import time
from pathlib import Path

# Create output directory if it doesn't exist
output_dir = os.path.join(os.getcwd(), "data_new")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_file = os.path.join(output_dir, "EEA.csv")


def download_source(url, output_path, chunk_size=512, max_retries=3):
    """Download file with retry logic and better error handling"""
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            print(f"🌐 Attempting download (attempt {attempt + 1}/{max_retries})...")
            
            # Start the download
            response = requests.get(url=url, stream=True, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            # Get file size if available
            file_size = response.headers.get('content-length')
            if file_size:
                file_size = int(file_size)
                print(f"📊 File size: {file_size:,} bytes ({file_size / (1024*1024):.2f} MB)")
            else:
                print("📊 File size: Unknown")
            
            # Download with progress tracking
            downloaded_size = 0
            print(f"💾 Downloading to: {output_path}")
            
            with open(output_path, mode='wb') as f:
                for chunk in response.iter_content(chunk_size):
                    if chunk:  # Filter out keep-alive chunks
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Show progress every 1MB
                        if downloaded_size % (1024 * 1024) == 0:
                            if file_size:
                                progress = (downloaded_size / file_size) * 100
                                print(f"📥 Downloaded: {downloaded_size:,} bytes ({progress:.1f}%)")
                            else:
                                print(f"📥 Downloaded: {downloaded_size:,} bytes")
            
            print(f"✅ Download completed successfully!")
            print(f"📂 Total downloaded: {downloaded_size:,} bytes")
            
            # Verify file was created and has content
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
                print(f"📋 File verification: ✅ Success ({file_size_mb:.2f} MB)")
                return True
            else:
                print("❌ File verification failed: File is empty or doesn't exist")
                return False
                
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout on attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10  # Progressive backoff: 10, 20, 30 seconds
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                
        except Exception as e:
            print(f"❌ Unexpected error on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
    
    print(f"❌ Failed to download after {max_retries} attempts")
    return False


print(f"🚀 Starting EEA data download")
print(f"📂 Output file: {output_file}")
print(f"🌍 Source: European Environment Agency (EEA)")
print("=" * 60)

# EEA Policy and Measures Database download URL
# This downloads all climate policies and measures from the EEA database
url = 'http://pam.apps.eea.europa.eu/tools/download?download_query=http%3A%2F%2Fpam.apps.eea.europa.eu%2F%3Fsource%3D%7B%22track_total_hits%22%3Atrue%2C%22query%22%3A%7B%22match_all%22%3A%7B%7D%7D%2C%22display_type%22%3A%22tabular%22%2C%22sort%22%3A%5B%7B%22Country%22%3A%7B%22order%22%3A%22asc%22%7D%7D%2C%7B%22ID_of_policy_or_measure%22%3A%7B%22order%22%3A%22asc%22%7D%7D%5D%2C%22highlight%22%3A%7B%22fields%22%3A%7B%22*%22%3A%7B%7D%7D%7D%7D&download_format=csv'

print("🔗 Downloading from EEA Policy and Measures Database...")
success = download_source(url, output_file)

if success:
    print(f"\n🎉 EEA download completed successfully!")
    print(f"📊 Data contains comprehensive European climate policies and measures")
    print(f"📂 File saved to: {output_file}")
    
    # Additional file information
    try:
        file_size = os.path.getsize(output_file)
        file_size_mb = file_size / (1024 * 1024)
        print(f"📋 Final file size: {file_size:,} bytes ({file_size_mb:.2f} MB)")
        
        # Try to count lines to estimate number of policies
        with open(output_file, 'r', encoding='utf-8') as f:
            line_count = sum(1 for _ in f)
        print(f"📊 Estimated policies: ~{line_count - 1:,} (excluding header)")
        
    except Exception as e:
        print(f"⚠️  Could not get additional file information: {e}")
else:
    print(f"\n❌ EEA download failed!")
    print(f"🔧 Please check your internet connection and try again")
    exit(1)
