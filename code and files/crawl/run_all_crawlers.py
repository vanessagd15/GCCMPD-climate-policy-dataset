#!/usr/bin/env python3
"""
Master Climate Policy Crawler Runner
====================================

This script runs all climate policy crawlers in sequence with a unified MIN_YEAR configuration.
It provides comprehensive logging, error handling, and progress tracking across all crawlers.

Usage:
    python run_all_crawlers.py [--min-year YYYY] [--include crawler1,crawler2] [--exclude crawler3,crawler4]

Example:
    python run_all_crawlers.py --min-year 2022
    python run_all_crawlers.py --include APEP,CRT --min-year 2021
    python run_all_crawlers.py --exclude MEE_PRC,iea_all_policy
"""

import os
import sys
import time
import subprocess
import argparse
from pathlib import Path
from datetime import datetime
import json

# Configuration
DEFAULT_MIN_YEAR = 2021
CRAWLER_DIR = Path(__file__).parent
OUTPUT_DIR = CRAWLER_DIR / '../data_new'

# Define all available crawlers with their characteristics
CRAWLERS = {
    'APEP': {
        'file': 'APEP_crawl.py',
        'description': 'APEP Climate Policy Database',
        'estimated_time': '5-10 minutes',
        'requires_selenium': False,
        'data_source': 'APEP Database'
    },
    'CDR_CCUS': {
        'file': 'CDR_CCUS_crawl.py',
        'description': 'Carbon Dioxide Removal & CCUS Policies',
        'estimated_time': '3-5 minutes',
        'requires_selenium': False,
        'data_source': 'CDR/CCUS Database'
    },
    'CDR_NETS': {
        'file': 'CDR_NETS_crawl.py',
        'description': 'CDR Negative Emissions Technologies',
        'estimated_time': '3-5 minutes',
        'requires_selenium': False,
        'data_source': 'CDR NETS Database'
    },
    'CRT': {
        'file': 'CRT_crawl.py',
        'description': 'Climate Risk & Technology Policies',
        'estimated_time': '10-15 minutes',
        'requires_selenium': False,
        'data_source': 'CRT Database'
    },
    'ECOLEX_Legislation': {
        'file': 'ECOLEX_Legislation_crawl.py',
        'description': 'ECOLEX Environmental Legislation (Multithreaded)',
        'estimated_time': '15-25 minutes',
        'requires_selenium': False,
        'data_source': 'ECOLEX Database'
    },
    'ECOLEX_Legislation_NonThread': {
        'file': 'Non_Thread_ECOLEX_Legislation_crawl.py',
        'description': 'ECOLEX Environmental Legislation (Single-threaded)',
        'estimated_time': '30-45 minutes',
        'requires_selenium': False,
        'data_source': 'ECOLEX Database'
    },
    'ECOLEX_Treaty': {
        'file': 'ECOLEX_Treaty_crawl.py',
        'description': 'ECOLEX Environmental Treaties',
        'estimated_time': '10-15 minutes',
        'requires_selenium': False,
        'data_source': 'ECOLEX Database'
    },
    'EEA': {
        'file': 'EEA_crawl.py',
        'description': 'European Environment Agency Policies',
        'estimated_time': '5-8 minutes',
        'requires_selenium': False,
        'data_source': 'EEA Database'
    },
    'GOV_PRC': {
        'file': 'GOV_PRC_crawl.py',
        'description': 'Chinese Government Climate Policies',
        'estimated_time': '10-20 minutes',
        'requires_selenium': False,
        'data_source': 'Chinese Government'
    },
    'ICAP_ETS': {
        'file': 'ICAP_ETS_crawl.py',
        'description': 'ICAP Emissions Trading Systems',
        'estimated_time': '8-12 minutes',
        'requires_selenium': False,
        'data_source': 'ICAP Database'
    },
    'IEA': {
        'file': 'iea_all_policy_crawl.py',
        'description': 'IEA Climate Policy Database (Complex)',
        'estimated_time': '20-30 minutes',
        'requires_selenium': False,
        'data_source': 'IEA Database'
    },
    'MEE_PRC': {
        'file': 'MEE_PRC_crawl.py',
        'description': 'Chinese Ministry of Ecology Policies',
        'estimated_time': '15-25 minutes',
        'requires_selenium': False,
        'data_source': 'MEE China'
    },
    'LSE_CP_Download': {
        'file': 'lse_and_cp_download.py',
        'description': 'LSE Climate Laws Database Download',
        'estimated_time': '2-3 minutes',
        'requires_selenium': False,
        'data_source': 'LSE Database'
    },
    'Climate_Policy_Download': {
        'file': 'cp_download.py',
        'description': 'Climate Policy Database Download',
        'estimated_time': '3-5 minutes',
        'requires_selenium': True,
        'data_source': 'Climate Policy Database'
    }
}

# Global tracking variables
total_crawlers = 0
successful_crawlers = 0
failed_crawlers = 0
skipped_crawlers = 0
start_time = None
crawler_results = {}


def print_banner():
    """Print application banner"""
    print("=" * 80)
    print("🌍 CLIMATE POLICY CRAWLER SUITE")
    print("🎯 Comprehensive Multi-Source Climate Policy Data Collection")
    print("=" * 80)


def print_crawler_summary():
    """Print summary of available crawlers"""
    print(f"\n📋 Available Crawlers ({len(CRAWLERS)} total):")
    print("-" * 80)
    
    for name, info in CRAWLERS.items():
        selenium_indicator = "🚗" if info['requires_selenium'] else "🌐"
        print(f"{selenium_indicator} {name:<25} | {info['description']:<40} | {info['estimated_time']}")
    
    print("-" * 80)
    print("🌐 = HTTP/Requests based  |  🚗 = Selenium WebDriver required")


def update_crawler_min_year(crawler_file, min_year):
    """Update the MIN_YEAR configuration in a crawler file"""
    crawler_path = CRAWLER_DIR / crawler_file
    
    if not crawler_path.exists():
        print(f"⚠️  Crawler file not found: {crawler_file}")
        return False
    
    try:
        # Read current content
        with open(crawler_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Update MIN_YEAR
        import re
        pattern = r'MIN_YEAR\s*=\s*\d+'
        replacement = f'MIN_YEAR = {min_year}'
        
        if re.search(pattern, content):
            updated_content = re.sub(pattern, replacement, content)
            
            # Write back if changed
            if updated_content != content:
                with open(crawler_path, 'w', encoding='utf-8') as f:
                    f.write(updated_content)
                print(f"✅ Updated {crawler_file}: MIN_YEAR = {min_year}")
            else:
                print(f"ℹ️  {crawler_file}: MIN_YEAR already set to {min_year}")
            return True
        else:
            print(f"⚠️  {crawler_file}: No MIN_YEAR configuration found")
            return False
            
    except Exception as e:
        print(f"❌ Error updating {crawler_file}: {e}")
        return False


def run_crawler(crawler_name, crawler_info):
    """Run a single crawler with comprehensive error handling"""
    global successful_crawlers, failed_crawlers, crawler_results
    
    print(f"\n{'='*60}")
    print(f"🚀 Starting: {crawler_name}")
    print(f"📝 Description: {crawler_info['description']}")
    print(f"⏱️  Estimated time: {crawler_info['estimated_time']}")
    print(f"📊 Data source: {crawler_info['data_source']}")
    print(f"{'='*60}")
    
    crawler_path = CRAWLER_DIR / crawler_info['file']
    start_time = time.time()
    
    try:
        # Check if crawler file exists
        if not crawler_path.exists():
            raise FileNotFoundError(f"Crawler file not found: {crawler_info['file']}")
        
        # Run the crawler
        print(f"🔄 Executing: {crawler_info['file']}")
        result = subprocess.run(
            [sys.executable, str(crawler_path)],
            cwd=str(CRAWLER_DIR),
            capture_output=True,
            text=True,
            timeout=7200  # 2 hour timeout per crawler
        )
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✅ {crawler_name} completed successfully!")
            print(f"⏱️  Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
            successful_crawlers += 1
            crawler_results[crawler_name] = {
                'status': 'success',
                'duration': duration,
                'output': result.stdout[-1000:] if result.stdout else '',  # Last 1000 chars
                'error': ''
            }
        else:
            print(f"❌ {crawler_name} failed with return code {result.returncode}")
            print(f"⏱️  Duration: {duration:.1f} seconds")
            if result.stderr:
                print(f"🔍 Error output (last 500 chars):")
                print(result.stderr[-500:])
            failed_crawlers += 1
            crawler_results[crawler_name] = {
                'status': 'failed',
                'duration': duration,
                'output': result.stdout[-500:] if result.stdout else '',
                'error': result.stderr[-500:] if result.stderr else 'Unknown error'
            }
            
    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        print(f"⏰ {crawler_name} timed out after {duration:.1f} seconds")
        failed_crawlers += 1
        crawler_results[crawler_name] = {
            'status': 'timeout',
            'duration': duration,
            'output': '',
            'error': 'Process timed out after 2 hours'
        }
    except Exception as e:
        duration = time.time() - start_time
        print(f"❌ {crawler_name} crashed: {e}")
        failed_crawlers += 1
        crawler_results[crawler_name] = {
            'status': 'crashed',
            'duration': duration,
            'output': '',
            'error': str(e)
        }


def save_execution_report():
    """Save a detailed execution report"""
    try:
        report = {
            'execution_date': datetime.now().isoformat(),
            'total_duration': time.time() - start_time if start_time else 0,
            'summary': {
                'total_crawlers': total_crawlers,
                'successful': successful_crawlers,
                'failed': failed_crawlers,
                'skipped': skipped_crawlers
            },
            'crawler_results': crawler_results
        }
        
        report_file = OUTPUT_DIR / f"crawler_execution_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        OUTPUT_DIR.mkdir(exist_ok=True)
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        print(f"📄 Execution report saved: {report_file}")
        
    except Exception as e:
        print(f"⚠️  Could not save execution report: {e}")


def print_final_summary():
    """Print comprehensive final summary"""
    total_duration = time.time() - start_time if start_time else 0
    
    print(f"\n{'='*80}")
    print("🎉 CLIMATE POLICY CRAWLER SUITE - EXECUTION COMPLETED")
    print(f"{'='*80}")
    print(f"⏱️  Total execution time: {total_duration:.1f} seconds ({total_duration/60:.1f} minutes)")
    print(f"📊 Summary:")
    print(f"   ✅ Successful: {successful_crawlers} crawlers")
    print(f"   ❌ Failed: {failed_crawlers} crawlers")
    print(f"   ⏭️  Skipped: {skipped_crawlers} crawlers")
    print(f"   📈 Success rate: {(successful_crawlers/total_crawlers*100):.1f}%" if total_crawlers > 0 else "   📈 Success rate: N/A")
    
    if successful_crawlers > 0:
        print(f"\n✅ Successful crawlers:")
        for name, result in crawler_results.items():
            if result['status'] == 'success':
                print(f"   • {name} ({result['duration']:.1f}s)")
    
    if failed_crawlers > 0:
        print(f"\n❌ Failed crawlers:")
        for name, result in crawler_results.items():
            if result['status'] in ['failed', 'timeout', 'crashed']:
                print(f"   • {name} ({result['status']}) - {result['error'][:100]}...")
    
    print(f"\n📂 Output directory: {OUTPUT_DIR.absolute()}")
    print(f"💡 Next steps:")
    print(f"   1. Review individual crawler outputs in {OUTPUT_DIR}")
    print(f"   2. Check execution report for detailed logs")
    print(f"   3. Merge collected data for analysis")
    print(f"{'='*80}")


def main():
    """Main execution function"""
    global total_crawlers, start_time, skipped_crawlers
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Run climate policy crawlers in sequence',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--min-year', type=int, default=DEFAULT_MIN_YEAR,
                        help=f'Minimum year for data collection (default: {DEFAULT_MIN_YEAR})')
    parser.add_argument('--include', type=str, help='Comma-separated list of crawlers to include')
    parser.add_argument('--exclude', type=str, help='Comma-separated list of crawlers to exclude')
    parser.add_argument('--list', action='store_true', help='List available crawlers and exit')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be executed without running')
    
    args = parser.parse_args()
    
    print_banner()
    
    if args.list:
        print_crawler_summary()
        return
    
    # Determine which crawlers to run
    crawlers_to_run = dict(CRAWLERS)
    
    if args.include:
        included = [name.strip() for name in args.include.split(',')]
        crawlers_to_run = {name: info for name, info in CRAWLERS.items() if name in included}
        missing = set(included) - set(crawlers_to_run.keys())
        if missing:
            print(f"⚠️  Unknown crawlers specified in --include: {', '.join(missing)}")
            print("Use --list to see available crawlers")
            return
    
    if args.exclude:
        excluded = [name.strip() for name in args.exclude.split(',')]
        crawlers_to_run = {name: info for name, info in crawlers_to_run.items() if name not in excluded}
        skipped_crawlers = len(excluded)
    
    total_crawlers = len(crawlers_to_run)
    
    if total_crawlers == 0:
        print("❌ No crawlers selected to run")
        return
    
    print(f"🎯 Configuration:")
    print(f"   📅 MIN_YEAR: {args.min_year}")
    print(f"   🔢 Crawlers to run: {total_crawlers}")
    print(f"   📂 Output directory: {OUTPUT_DIR.absolute()}")
    
    if args.dry_run:
        print(f"\n🔍 DRY RUN - Would execute these crawlers:")
        for name, info in crawlers_to_run.items():
            print(f"   • {name}: {info['description']}")
        return
    
    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Update MIN_YEAR in all crawler files
    print(f"\n🔧 Updating MIN_YEAR configuration to {args.min_year}...")
    for name, info in crawlers_to_run.items():
        update_crawler_min_year(info['file'], args.min_year)
    
    # Estimate total time
    print(f"\n⏱️  Estimated total execution time: 2-4 hours (depending on network and data volume)")
    
    # Confirm execution
    try:
        response = input(f"\n🚀 Ready to run {total_crawlers} crawlers. Continue? (y/N): ").strip().lower()
        if response not in ['y', 'yes']:
            print("❌ Execution cancelled by user")
            return
    except KeyboardInterrupt:
        print("\n❌ Execution cancelled by user")
        return
    
    start_time = time.time()
    
    # Run crawlers in sequence
    print(f"\n🚀 Starting crawler execution sequence...")
    try:
        for i, (name, info) in enumerate(crawlers_to_run.items(), 1):
            print(f"\n📊 Progress: {i}/{total_crawlers} crawlers")
            run_crawler(name, info)
            
            # Brief pause between crawlers
            if i < total_crawlers:
                print(f"⏸️  Brief pause before next crawler...")
                time.sleep(3)
                
    except KeyboardInterrupt:
        print(f"\n⚠️  Execution interrupted by user")
    except Exception as e:
        print(f"\n❌ Critical error during execution: {e}")
    finally:
        # Always generate summary and report
        print_final_summary()
        save_execution_report()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n⚠️  Program interrupted by user")
        if start_time:
            print_final_summary()
            save_execution_report()
    except Exception as e:
        print(f"❌ Critical error: {e}")
        if start_time:
            print_final_summary()
            save_execution_report()
