#!/usr/bin/env python3
"""
LSE Climate Change Laws Database Access Information
==================================================

⚠️  NOTE: This crawler does NOT work automatically
The LSE CCLW database requires manual download with personal registration.

✅ MANUAL DOWNLOAD COMPLETED:
   📁 File: data_new/CCLW_document_data_download-2026-02-23.zip
   📝 Process: 
      1. Visit form at: https://form.jotform.com/233131638610347
      2. Fill out with personal details and research purpose
      3. Download the zip file manually
      4. Place in data_new directory

IMPORTANT: As of 2025, the LSE Climate Change Laws of the World database 
no longer provides direct CSV downloads. Access requires form submission.
"""

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


def print_lse_access_instructions():
    """Print instructions for accessing LSE Climate Laws database"""
    print("📋 LSE CLIMATE CHANGE LAWS DATABASE ACCESS")
    print("=" * 60)
    print("🔒 The LSE database now requires form-based access (no direct downloads)")
    print()
    print("📝 To access the data:")
    print("   1. Visit: https://form.jotform.com/233131638610347")
    print("   2. Fill out the data request form with:")
    print("      • Your contact information")
    print("      • Research purpose/use case")
    print("      • Preferred data format (CSV)")
    print("   3. Submit form and wait for email with download link")
    print()
    print("⏰ Expected response time: 1-3 business days")
    print("📧 Contact: support@climatepolicyradar.org for issues")
    print()
    print("🌐 Database website: https://climate-laws.org/")
    print("📊 Contains: ~1,393 laws + ~3,622 policies from 196+ countries")
    print("=" * 60)


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


def get_alternative_climate_databases():
    """Get list of alternative climate policy databases with direct access"""
    return [
        {
            'url': 'https://climatepolicyinitiative.org/wp-content/uploads/2021/10/Global-Landscape-of-Climate-Finance-2021.csv',
            'filename': 'climate_policy_initiative_finance.csv',
            'description': 'Climate Policy Initiative - Global Finance Data',
            'active': False  # Check if URL is active
        },
        # Add more alternative sources here as they become available
    ]


def main():
    """Main function providing LSE access info and alternative downloads"""
    global downloaded_count, failed_count
    
    print("=" * 60)
    print("🌍 Climate Policy Database Access Tool")
    print("🎯 Enhanced version with current access methods")
    print("=" * 60)
    print(f"📂 Output directory: {output_dir.absolute()}")
    print(f"📅 Data filtering: {MIN_YEAR} onwards (applied during processing)")
    
    # Show LSE access instructions first
    print_lse_access_instructions()
    
    # Check for alternative databases
    alternative_dbs = get_alternative_climate_databases()
    active_alternatives = [db for db in alternative_dbs if db.get('active', True)]
    
    if active_alternatives:
        print(f"\n🔄 ALTERNATIVE CLIMATE DATABASES")
        print("=" * 60)
        print(f"Found {len(active_alternatives)} alternative database(s) with direct access:")
        
        for index, config in enumerate(active_alternatives):
            print(f"\n📋 Processing {index + 1}/{len(active_alternatives)}: {config['description']}")
            
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
    else:
        print(f"\n⚠️  No alternative databases currently available for direct download.")
        print(f"📝 Please use the LSE form-based access method above.")
    
    # Final summary
    print(f"\n🎉 Process completed!")
    print(f"📊 Statistics:")
    if active_alternatives:
        print(f"   ✅ Successfully downloaded: {downloaded_count} alternative database files")
        print(f"   ❌ Failed downloads: {failed_count} files")
    print(f"   � LSE database: Requires form-based access (see instructions above)")
    print(f"📂 Files saved to: {output_dir.absolute()}")
    
    print(f"\n💡 Next steps:")
    print(f"   1. Submit LSE data request form for comprehensive climate laws database")
    print(f"   2. Review any downloaded alternative databases")
    print(f"   3. Apply year filtering (>= {MIN_YEAR}) during data processing")
    print(f"   4. Integrate with main climate policy crawler results")
    
    print(f"\n🔗 Useful links:")
    print(f"   • LSE Climate Laws: https://climate-laws.org/")
    print(f"   • Data Request Form: https://form.jotform.com/233131638610347")
    print(f"   • Contact Support: support@climatepolicyradar.org")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Process interrupted by user")
        print(f"📊 Progress so far:")
        print(f"   ✅ Downloaded: {downloaded_count} files")
        print(f"   ❌ Failed: {failed_count} files")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        print(f"📊 Progress before error:")
        print(f"   ✅ Downloaded: {downloaded_count} files")
        print(f"   ❌ Failed: {failed_count} files")
