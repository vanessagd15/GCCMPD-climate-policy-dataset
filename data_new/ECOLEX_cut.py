import pandas as pd
import math
from tqdm import tqdm
import os


def cut_df(file_name, n, output_dir=None):
    df = pd.read_csv(file_name)
    df_num = len(df)
    every_epoch_num = math.floor((df_num / n))
    print(f"📂 Reading: {file_name}")
    print(f"📊 Total rows: {df_num}")
    print(f"📑 Splitting into {n} chunks...")
    print(f"📁 Saving to: {output_dir if output_dir else 'current directory'}")
    
    for index in tqdm(range(n)):
        if output_dir:
            output_file = os.path.join(output_dir, f'ECOLEX_Legislation_{index}.csv')
        else:
            output_file = f'ECOLEX_Legislation_{index}.csv'
        
        if index < n - 1:
            df_tem = df[every_epoch_num * index: every_epoch_num * (index + 1)]
        else:
            df_tem = df[every_epoch_num * index:]
        df_tem.to_csv(output_file, index=False)


if __name__ == '__main__':
    # Run from within data_new directory
    import os
    current_dir = os.getcwd()
    data_new_dir = os.path.join(current_dir, 'data_new')
    
    # If we're already in data_new, use current dir, otherwise use data_new
    if 'data_new' in current_dir:
        input_file = 'ECOLEX_Legislation.csv'
        output_dir = None  # Save in current directory (data_new)
    else:
        input_file = os.path.join('data_new', 'ECOLEX_Legislation.csv')
        output_dir = 'data_new'  # Save in data_new directory
    
    cut_df(input_file, 40, output_dir)
    print("🎉 All files created successfully!")

