import requests
import shutil
import os
from urllib.parse import urlparse
import time
from pathlib import Path

# Configuration
MIN_YEAR = 2021
MAX_RETRIES = 3
RETRY_DELAY = 2
CHUNK_SIZE = 8192

# Create output directory
output_dir = Path('../data_new')
output_dir.mkdir(exist_ok=True)

# Global counters for tracking
downloaded_count = 0
failed_count = 0


def download_source(url, output_path, chunk_size=CHUNK_SIZE):
    """Download a file from URL with robust error handling and progress tracking"""
    global downloaded_count, failed_count
    
    print(f"🌐 Downloading: {url}")
    print(f"📂 Target: {output_path}")
    
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/csv,application/csv,text/plain,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"🔄 Attempt {attempt + 1}/{MAX_RETRIES}")
            
            # Make request with timeout
            response = requests.get(
                url=url, 
                stream=True, 
                headers=headers, 
                timeout=(10, 60),  # (connection, read) timeout
                verify=True
            )
            response.raise_for_status()
            
            # Get file size if available
            file_size = response.headers.get('content-length')
            if file_size:
                file_size = int(file_size)
                print(f"📊 File size: {file_size:,} bytes")
            
            # Download with progress tracking
            downloaded_bytes = 0
            with open(output_path, mode='wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded_bytes += len(chunk)
                        
                        # Show progress every MB
                        if downloaded_bytes % (1024 * 1024) == 0:
                            print(f"📥 Downloaded: {downloaded_bytes:,} bytes")
            
            print(f"✅ Successfully downloaded: {output_path} ({downloaded_bytes:,} bytes)")
            downloaded_count += 1
            return True
            
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout on attempt {attempt + 1}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        except requests.exceptions.ConnectionError:
            print(f"🔌 Connection error on attempt {attempt + 1}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        except requests.exceptions.HTTPError as e:
            print(f"❌ HTTP error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        except Exception as e:
            print(f"❌ Unexpected error on attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
    
    print(f"❌ Failed to download after {MAX_RETRIES} attempts: {url}")
    failed_count += 1
    return False


def main():
    """Main function to download climate policy databases"""
    print("=" * 60)
    print("🌍 LSE & Climate Policy Database Downloader")
    print("🎯 Enhanced version with robust error handling")
    print("=" * 60)
    print(f"📂 Output directory: {output_dir.absolute()}")
    print(f"📅 Data filtering: {MIN_YEAR} onwards (applied during processing)")
    
    # URLs and corresponding filenames
    download_configs = [
        {
            'url': 'https://climate-laws.org/legislation_and_policies.csv',
            'filename': 'lse_climate_laws.csv',
            'description': 'LSE Climate Laws Database'
        }
    ]
    
    print(f"\n🚀 Starting download of {len(download_configs)} database(s)...")
    
    for index, config in enumerate(download_configs):
        print(f"\n📋 Processing {index + 1}/{len(download_configs)}: {config['description']}")
        
        try:
            # Create full output path
            output_path = output_dir / config['filename']
            
            # Download the file
            success = download_source(config['url'], output_path)
            
            if success:
                # Verify file was created and has content
                if output_path.exists() and output_path.stat().st_size > 0:
                    print(f"✅ Verified: {config['filename']} ({output_path.stat().st_size:,} bytes)")
                else:
                    print(f"⚠️  Warning: File appears empty or missing: {config['filename']}")
                    failed_count += 1
            
        except Exception as e:
            print(f"❌ Critical error processing {config['description']}: {e}")
            continue
    
    # Final summary
    print(f"\n🎉 Download process completed!")
    print(f"📊 Final Statistics:")
    print(f"   ✅ Successfully downloaded: {downloaded_count} files")
    print(f"   ❌ Failed downloads: {failed_count} files")
    print(f"📂 All files saved to: {output_dir.absolute()}")
    
    if downloaded_count > 0:
        print(f"\n💡 Next steps:")
        print(f"   1. Review downloaded CSV files for data quality")
        print(f"   2. Apply year filtering (>= {MIN_YEAR}) during data processing")
        print(f"   3. Integrate with main climate policy database")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Download interrupted by user")
        print(f"📊 Progress so far:")
        print(f"   ✅ Downloaded: {downloaded_count} files")
        print(f"   ❌ Failed: {failed_count} files")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        print(f"📊 Progress before error:")
        print(f"   ✅ Downloaded: {downloaded_count} files")
        print(f"   ❌ Failed: {failed_count} files")
