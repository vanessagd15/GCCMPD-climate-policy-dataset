#!/usr/bin/env python3
"""
Quick Crawler Results Merger
============================

A simple script to quickly merge crawler results from two directories,
combining files from the same sources and removing obvious duplicates.
"""

import pandas as pd
import os
from pathlib import Path
from datetime import datetime

# Simple configuration
OLD_DIR = Path('../files')
NEW_DIR = Path('../../data_new') 
OUTPUT_DIR = Path('../files_merged_simple')

# Files to merge (same source files)
FILES_TO_MERGE = [
    'APEP.csv',
    'CDR_CCUS.csv', 
    'CDR_NETS.csv',
    'CRT.csv',
    'ECOLEX_Legislation.csv',
    'EEA.csv',
    'GOV_PRC.csv',
    'ICAP_ETS.csv',
    'MEE_PRC.csv'
]

def simple_merge():
    """Simple merge function"""
    print("🔄 Quick Crawler Results Merger")
    print("=" * 50)
    
    OUTPUT_DIR.mkdir(exist_ok=True)
    merged_count = 0
    
    for filename in FILES_TO_MERGE:
        print(f"\n📄 Processing: {filename}")
        
        old_file = OLD_DIR / filename
        new_file = NEW_DIR / filename
        
        dataframes = []
        
        # Load old file if exists
        if old_file.exists():
            try:
                old_df = pd.read_csv(old_file, encoding='utf-8-sig')
                old_df['_source'] = 'old'
                dataframes.append(old_df)
                print(f"  📚 Old: {len(old_df)} records")
            except Exception as e:
                print(f"  ⚠️  Error loading old file: {e}")
        
        # Load new file if exists  
        if new_file.exists():
            try:
                new_df = pd.read_csv(new_file, encoding='utf-8-sig')
                new_df['_source'] = 'new'
                dataframes.append(new_df)
                print(f"  🆕 New: {len(new_df)} records")
            except Exception as e:
                print(f"  ⚠️  Error loading new file: {e}")
        
        # Merge if we have data
        if dataframes:
            merged_df = pd.concat(dataframes, ignore_index=True)
            
            # Simple duplicate removal based on first few columns
            initial_len = len(merged_df)
            
            # Try to find a URL or title column for deduplication
            dedupe_cols = []
            for col in ['URL', 'url', 'Policy', 'policy', 'title']:
                if col in merged_df.columns:
                    dedupe_cols.append(col)
                    break
            
            if dedupe_cols:
                # Keep newest data (new source first)
                merged_df = merged_df.sort_values('_source', ascending=False)
                merged_df = merged_df.drop_duplicates(subset=dedupe_cols, keep='first')
            
            # Remove helper column
            merged_df = merged_df.drop('_source', axis=1, errors='ignore')
            
            # Save merged file
            output_file = OUTPUT_DIR / filename
            merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            final_len = len(merged_df)
            duplicates = initial_len - final_len
            
            print(f"  ✅ Merged: {final_len} records (removed {duplicates} duplicates)")
            merged_count += 1
        else:
            print(f"  ❌ No data found for {filename}")
    
    print(f"\n🎉 Merge completed!")
    print(f"📊 Files processed: {merged_count}/{len(FILES_TO_MERGE)}")
    print(f"📂 Output: {OUTPUT_DIR.absolute()}")

if __name__ == '__main__':
    simple_merge()
