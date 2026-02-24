import time
import requests
import os
from pathlib import Path

def simple_climate_policy_download():
    """Simplified climate policy database downloader"""
    
    # Create output directory - fixed path
    output_dir = Path(__file__).parent.parent.parent / "data_new"
    output_dir.mkdir(exist_ok=True)
    
    print("🌍 Simple Climate Policy Database Downloader")
    print("=" * 50)
    print(f"📂 Output directory: {output_dir}")
    
    # Try the most common direct download URLs
    urls_to_try = [
        "https://climatepolicydatabase.org/policies.csv",
        "https://climatepolicydatabase.org/export.csv", 
        "https://climatepolicydatabase.org/policies/export.csv",
        "https://climatepolicydatabase.org/api/policies.csv",
        "https://climatepolicydatabase.org/data/policies.csv"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/csv,application/csv,text/plain,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://climatepolicydatabase.org/'
    }
    
    for url in urls_to_try:
        try:
            print(f"\n🔄 Trying: {url}")
            
            session = requests.Session()
            response = session.get(url, headers=headers, timeout=30, allow_redirects=True)
            
            print(f"   Status: {response.status_code}")
            print(f"   Content-Type: {response.headers.get('content-type', 'unknown')}")
            print(f"   Size: {len(response.content)} bytes")
            
            if response.status_code == 200 and len(response.content) > 1000:
                # Quick check if it's actual CSV data
                text_sample = response.content[:500].decode('utf-8', errors='ignore')
                
                if not text_sample.strip().startswith('<'):  # Not HTML
                    filename = f"climate_policy_database_{time.strftime('%Y%m%d_%H%M%S')}.csv"
                    filepath = output_dir / filename
                    
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    
                    print(f"✅ SUCCESS! Downloaded: {filename}")
                    print(f"📄 File size: {filepath.stat().st_size:,} bytes")
                    print(f"📁 Saved to: {filepath}")
                    return filepath
                else:
                    print("   ❌ Got HTML instead of CSV")
            else:
                print(f"   ❌ Failed: Status {response.status_code} or too small")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print("\n❌ All automatic download attempts failed")
    print("\n🎯 MANUAL DOWNLOAD INSTRUCTIONS:")
    print("1. Open Chrome browser")
    print("2. Go to: https://climatepolicydatabase.org/policies")
    print("3. Look for Export or Download button")
    print("4. Download the CSV file")
    print("5. Copy downloaded file to:")
    print(f"   {output_dir}")
    print("6. Rename to: climate_policy_database_YYYYMMDD_HHMMSS.csv")
    
    return None

if __name__ == "__main__":
    result = simple_climate_policy_download()
    if result:
        print(f"\n🎉 Download completed successfully!")
    else:
        print(f"\n⚠️ Please download manually and place in data_new directory")