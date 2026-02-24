"""
IEA Policy Deduplication Script
================================
Removes duplicate policies from the IEA (International Energy Agency) dataset

⚠️  UPDATE: Modified input file format and naming
- Changed to read from: IEA_all_policy.csv (CSV format)
- Updated to use standard field names for consistency
- Output: iea_dedup_result.xlsx
"""

import pandas as pd
import numpy as np
import os
import sys

print("================== {} ==================".format(os.path.basename(sys.argv[0])))


def get_data():
    data = pd.read_csv("policy_db_iea_cp_cclw_update/IEA_all_policy.csv")
    return data


def data_process(df):
    #df.fillna('', inplace=True)
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].fillna('')
    # Force problematic text columns to safe string dtype
    text_cols = ["Sectors", "Technologies"]
    
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    # group by "Country ISO", "Date of decision", "Jurisdiction", "Policy Title"
    # then concat specified field like "Type of policy instrument" to the first data
    second_filter = df.groupby(["Country", "Year", "Jurisdiction", "Policy"])["Topics"].apply(
        lambda x: ";".join(set(x.str.cat(sep=";").split(";")))).reset_index()
    second_filter_2 = df.groupby(["Country", "Year", "Jurisdiction", "Policy"])["Type"].apply(
        lambda x: ";".join(set(x.str.cat(sep=";").split(";")))).reset_index()
    second_filter_3 = df.groupby(["Country", "Year", "Jurisdiction", "Policy"])["Sectors"].apply(
        lambda x: ";".join(set(x.str.cat(sep=";").split(";")))).reset_index()
    second_filter_4 = df.groupby(["Country", "Year", "Jurisdiction", "Policy"])[
        "Technologies"].apply(
        lambda x: ";".join(set(x.str.cat(sep=";").split(";")))).reset_index()
    second_filter_5 = df.groupby(["Country", "Year", "Jurisdiction", "Policy"])[
        "Policy_Content"].apply(
        lambda x: "\n".join(set(x.str.cat(sep="¥¥").split("¥¥")))).reset_index()
    # second_filter_6 = df.groupby(["Country", "Year", "Jurisdiction"])["Policy"].apply(
    #     lambda x: ";".join(set(x.str.cat(sep=";").split(";")))).reset_index()
    second_filter["Type"] = second_filter_2["Type"]
    second_filter["Sectors"] = second_filter_3["Sectors"]
    second_filter["Technologies"] = second_filter_4["Technologies"]
    second_filter["Policy_Content"] = second_filter_5["Policy_Content"]
    # second_filter["Policy"] = second_filter_6["Policy"]
    # print(second_filter.info())

    # drop_duplicates by "Country ISO", "Date of decision", "Jurisdiction", "Policy Title"
    third_filter = df.drop_duplicates(subset=["Country", "Year", "Jurisdiction", "Policy"],
                                      keep="first")
    # print(third_filter.info())

    # drop columns "Topics","Type","Sectors","Technologies","Policy_Content"
    fourth_filter = third_filter.drop(["Topics", "Type", "Sectors", "Technologies", "Policy_Content"], axis=1)

    # print(fourth_filter)
    # merge by "Country ISO", "Date of decision", "Jurisdiction", "Policy Title"
    result_filter = pd.merge(second_filter, fourth_filter,
                             on=["Country", "Year", "Jurisdiction", "Policy"])

    result_filter.to_excel('iea_dedup_result.xlsx', index=False)

    with open("dup_statistic.txt", 'w') as f:
        f.write("====================Single Database Dedup: iea_dedup.py cp_dedup.py====================" + '\n')
        f.write('\n')
        f.write("IEA Raw: " + str(len(df)) + '\n')
        f.write("IEA After iea_dedup.py: " + str(len(result_filter)) + '\n')
        f.write("IEA first dup: " + str(len(df) - len(result_filter)) + '\n')
        f.write('\n')


if __name__ == '__main__':
    iea_df = get_data()
    print(len(iea_df))
    data_process(iea_df)
