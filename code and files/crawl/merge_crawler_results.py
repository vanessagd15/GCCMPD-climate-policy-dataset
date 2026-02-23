#!/usr/bin/env python3
"""
Crawler Results Merger
======================

This script merges climate policy data from the same websites collected at different times.
It combines data from 'code and files/files' (older results) and 'data_new' (newer results),
removing duplicates and creating consolidated datasets ready for translation processing.

Usage:
    python merge_crawler_results.py [--dry-run] [--verbose]

Features:
- Intelligent duplicate detection based on URL and policy title
- Data quality validation and reporting
- Backup creation before merging
- Comprehensive logging and statistics
"""

import pandas as pd
import os
from pathlib import Path
import shutil
import hashlib
from datetime import datetime
import argparse

# Configuration
SOURCE_DIRS = {
    'old_results': Path('../files'),      # Original results directory
    'new_results': Path('../../data_new') # New enhanced crawler results
}
OUTPUT_DIR = Path('../files_merged')      # Merged results output
BACKUP_DIR = Path('../files_backup')     # Backup of original files

# Website mappings - files from same sources
WEBSITE_MAPPINGS = {
    'APEP': {
        'files': ['APEP.csv'],
        'description': 'Australian Policy and Energy Portal'
    },
    'CDR_CCUS': {
        'files': ['CDR_CCUS.csv'],
        'description': 'Carbon Dioxide Removal & CCUS Policies'
    },
    'CDR_NETS': {
        'files': ['CDR_NETS.csv'],
        'description': 'CDR Negative Emissions Technologies'
    },
    'CRT': {
        'files': ['CRT.csv'],
        'description': 'Climate Risk & Technology Policies'
    },
    'ECOLEX_Legislation': {
        'files': ['ECOLEX_Legislation.csv'],
        'description': 'ECOLEX Environmental Legislation'
    },
    'ECOLEX_Treaty': {
        'files': ['ECOLEX_Treaty.csv'],
        'description': 'ECOLEX Environmental Treaties'
    },
    'EEA': {
        'files': ['EEA.csv'],
        'description': 'European Environment Agency'
    },
    'GOV_PRC': {
        'files': ['GOV_PRC.csv'],
        'description': 'Chinese Government Policies'
    },
    'ICAP_ETS': {
        'files': ['ICAP_ETS.csv'],
        'description': 'ICAP Emissions Trading Systems'
    },
    'MEE_PRC': {
        'files': ['MEE_PRC.csv'],
        'description': 'Chinese Ministry of Ecology'
    }
}

# Global statistics
merge_stats = {
    'processed_websites': 0,
    'total_old_records': 0,
    'total_new_records': 0,
    'total_merged_records': 0,
    'total_duplicates_removed': 0,
    'files_created': 0
}


def print_banner():
    """Print application banner"""
    print("=" * 80)
    print("🔄 CLIMATE POLICY CRAWLER RESULTS MERGER")
    print("🎯 Intelligent Data Consolidation from Multiple Crawler Runs")
    print("=" * 80)


def create_backup():
    """Create backup of original files directory"""
    if BACKUP_DIR.exists():
        shutil.rmtree(BACKUP_DIR)
    
    print(f"📦 Creating backup of original files...")
    shutil.copytree(SOURCE_DIRS['old_results'], BACKUP_DIR)
    print(f"✅ Backup created: {BACKUP_DIR.absolute()}")


def create_file_hash(row):
    """Create a hash for duplicate detection based on URL and title"""
    # Use URL as primary identifier, fall back to title if URL not available
    url = str(row.get('URL', row.get('policy_url', row.get('url', ''))))
    title = str(row.get('Policy', row.get('policy', row.get('title', ''))))
    
    # Create unique identifier
    identifier = f"{url}|{title}".lower().strip()
    return hashlib.md5(identifier.encode()).hexdigest()


def load_csv_safely(file_path):
    """Load CSV file with error handling and encoding detection"""
    try:
        # Try different encodings
        for encoding in ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                print(f"  📄 Loaded {len(df)} records from {file_path.name} (encoding: {encoding})")
                return df
            except UnicodeDecodeError:
                continue
        
        # If all encodings fail, try with error handling
        df = pd.read_csv(file_path, encoding='utf-8', errors='ignore')
        print(f"  ⚠️  Loaded {len(df)} records from {file_path.name} (with error handling)")
        return df
        
    except Exception as e:
        print(f"  ❌ Error loading {file_path.name}: {e}")
        return None


def standardize_columns(df, source_type):
    """Standardize column names across different crawler outputs"""
    # Common column mappings
    column_mappings = {
        'policy_url': 'URL',
        'url': 'URL',
        'policy': 'Policy',
        'title': 'Policy',
        'country': 'Country',
        'year': 'Year',
        'content': 'Policy_Content',
        'policy_content': 'Policy_Content',
        'abstract': 'Abstract',
        'source': 'Source'
    }
    
    # Apply mappings
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns and new_col not in df.columns:
            df = df.rename(columns={old_col: new_col})
    
    # Add source identifier if missing
    if 'Source' not in df.columns:
        df['Source'] = source_type
    
    return df


def merge_website_data(website_name, website_info, verbose=False):
    """Merge data from the same website collected at different times"""
    global merge_stats
    
    print(f"\n🌐 Processing: {website_name}")
    print(f"📝 Description: {website_info['description']}")
    
    old_data = None
    new_data = None
    
    # Load data from old results
    for filename in website_info['files']:
        old_file = SOURCE_DIRS['old_results'] / filename
        if old_file.exists():
            old_data = load_csv_safely(old_file)
            if old_data is not None:
                old_data = standardize_columns(old_data, website_name)
                merge_stats['total_old_records'] += len(old_data)
                if verbose:
                    print(f"  📊 Old data columns: {list(old_data.columns)}")
            break
    
    # Load data from new results  
    for filename in website_info['files']:
        new_file = SOURCE_DIRS['new_results'] / filename
        if new_file.exists():
            new_data = load_csv_safely(new_file)
            if new_data is not None:
                new_data = standardize_columns(new_data, website_name)
                merge_stats['total_new_records'] += len(new_data)
                if verbose:
                    print(f"  📊 New data columns: {list(new_data.columns)}")
            break
    
    # Determine merge strategy
    if old_data is None and new_data is None:
        print(f"  ⚠️  No data found for {website_name}")
        return False
    elif old_data is None:
        print(f"  🆕 Only new data available ({len(new_data)} records)")
        merged_data = new_data
    elif new_data is None:
        print(f"  📚 Only old data available ({len(old_data)} records)")
        merged_data = old_data
    else:
        print(f"  🔄 Merging old ({len(old_data)}) and new ({len(new_data)}) records")
        merged_data = merge_dataframes(old_data, new_data, website_name, verbose)
    
    # Save merged data
    if merged_data is not None and len(merged_data) > 0:
        output_file = OUTPUT_DIR / f"{website_name}.csv"
        merged_data.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        merge_stats['total_merged_records'] += len(merged_data)
        merge_stats['files_created'] += 1
        
        print(f"  ✅ Merged data saved: {output_file.name} ({len(merged_data)} records)")
        return True
    else:
        print(f"  ❌ No valid data to save for {website_name}")
        return False


def merge_dataframes(old_df, new_df, website_name, verbose=False):
    """Merge two dataframes removing duplicates intelligently"""
    
    # Add hash columns for duplicate detection
    old_df['_hash'] = old_df.apply(create_file_hash, axis=1)
    new_df['_hash'] = new_df.apply(create_file_hash, axis=1)
    
    # Add source tracking
    old_df['_data_source'] = 'old_crawl'
    new_df['_data_source'] = 'new_crawl'
    
    # Combine dataframes
    combined_df = pd.concat([old_df, new_df], ignore_index=True)
    
    # Remove duplicates, keeping new data when duplicates exist
    initial_count = len(combined_df)
    
    # Sort by source (new_crawl first) so new data is kept in case of duplicates
    combined_df = combined_df.sort_values('_data_source', ascending=True)
    combined_df = combined_df.drop_duplicates(subset=['_hash'], keep='first')
    
    final_count = len(combined_df)
    duplicates_removed = initial_count - final_count
    
    merge_stats['total_duplicates_removed'] += duplicates_removed
    
    print(f"  🔍 Duplicate analysis:")
    print(f"     • Combined records: {initial_count}")
    print(f"     • After deduplication: {final_count}")
    print(f"     • Duplicates removed: {duplicates_removed}")
    
    if verbose and duplicates_removed > 0:
        print(f"  📊 Data source breakdown:")
        source_counts = combined_df['_data_source'].value_counts()
        for source, count in source_counts.items():
            print(f"     • {source}: {count} records")
    
    # Remove helper columns
    combined_df = combined_df.drop(columns=['_hash', '_data_source'])
    
    return combined_df


def print_summary():
    """Print comprehensive merge summary"""
    print(f"\n{'='*80}")
    print("🎉 MERGER COMPLETED - SUMMARY REPORT")
    print(f"{'='*80}")
    print(f"📊 Processing Statistics:")
    print(f"   🌐 Websites processed: {merge_stats['processed_websites']}")
    print(f"   📄 Files created: {merge_stats['files_created']}")
    print(f"   📚 Total old records: {merge_stats['total_old_records']:,}")
    print(f"   🆕 Total new records: {merge_stats['total_new_records']:,}")
    print(f"   🔄 Total merged records: {merge_stats['total_merged_records']:,}")
    print(f"   🗑️  Duplicates removed: {merge_stats['total_duplicates_removed']:,}")
    
    if merge_stats['total_old_records'] + merge_stats['total_new_records'] > 0:
        dedup_rate = (merge_stats['total_duplicates_removed'] / 
                     (merge_stats['total_old_records'] + merge_stats['total_new_records'])) * 100
        print(f"   📈 Deduplication rate: {dedup_rate:.1f}%")
    
    print(f"\n📂 Output Directory: {OUTPUT_DIR.absolute()}")
    print(f"📦 Backup Directory: {BACKUP_DIR.absolute()}")
    
    print(f"\n💡 Next Steps:")
    print(f"   1. Review merged files in {OUTPUT_DIR}")
    print(f"   2. Update all_run.py to use merged data")
    print(f"   3. Run Stage 2 (translation) with consolidated datasets")
    print(f"   4. Original files backed up in {BACKUP_DIR}")
    print(f"{'='*80}")


def main():
    """Main execution function"""
    global merge_stats
    
    parser = argparse.ArgumentParser(
        description='Merge crawler results from different runs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be processed without actually merging')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed processing information')
    
    args = parser.parse_args()
    
    print_banner()
    
    # Check source directories
    print(f"📂 Source Directories:")
    for name, path in SOURCE_DIRS.items():
        abs_path = path.resolve()
        exists = abs_path.exists()
        print(f"   {name}: {abs_path} {'✅' if exists else '❌'}")
        if not exists:
            print(f"   ⚠️  Directory not found: {abs_path}")
    
    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)
    print(f"📁 Output directory: {OUTPUT_DIR.absolute()}")
    
    if args.dry_run:
        print(f"\n🔍 DRY RUN MODE - No files will be modified")
        print(f"Would process {len(WEBSITE_MAPPINGS)} websites:")
        for name, info in WEBSITE_MAPPINGS.items():
            print(f"   • {name}: {info['description']}")
        return
    
    # Create backup
    create_backup()
    
    # Process each website
    print(f"\n🚀 Starting merge process for {len(WEBSITE_MAPPINGS)} websites...")
    
    successful_merges = 0
    for website_name, website_info in WEBSITE_MAPPINGS.items():
        try:
            success = merge_website_data(website_name, website_info, args.verbose)
            if success:
                successful_merges += 1
            merge_stats['processed_websites'] += 1
        except Exception as e:
            print(f"❌ Error processing {website_name}: {e}")
            continue
    
    # Print summary
    print_summary()
    
    if successful_merges == 0:
        print("⚠️  No data was successfully merged. Please check source directories and file formats.")
    else:
        print(f"🎉 Successfully merged data from {successful_merges}/{len(WEBSITE_MAPPINGS)} websites!")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n⚠️  Merge process interrupted by user")
        print(f"📊 Progress so far: {merge_stats}")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        print(f"📊 Progress before error: {merge_stats}")
