import pandas as pd
import numpy as np
import os
import sys

print("================== {} ==================".format(os.path.basename(sys.argv[0])))


def get_data():
    #data = pd.read_csv("cp.csv")
    data = pd.read_csv("policy_db_iea_cp_cclw_update/climate_policy_database_policies.csv")
    return data


def data_process(df):
    #df.columns = df.columns.str.strip()
    #df.columns = df.columns.str.lower()
    #df["Date of decision"].fillna(0, inplace=True)
    #df["Date of decision"].fillna(0, inplace=True)
    if "decision_date" in df.columns:
        df["decision_date"] = df["decision_date"].fillna(0)

    #df["start_date"].fillna(0, inplace=True)
    if "start_date" in df.columns:
        df["start_date"] = df["start_date"].fillna(0)

    #df.fillna('', inplace=True)
    # Only fill text columns
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].fillna("").astype(str)

    date_list = [''] * len(df["decision_date"].to_list())
    for num, row in df.iterrows():
        if row["decision_date"]:
            date_list[num] = row["decision_date"]
        else:
            date_list[num] = row["start_date"]
    df["decision_date"] = date_list
    # group by "Country ISO", "Date of decision", "jurisdiction", "policy_name" 
    # then concat specified field like "policy_instrument" to the first data
    second_filter = \
        df.groupby(["country_iso", "decision_date", "jurisdiction", "policy_name"])[
            "policy_instrument"].apply(lambda x: ";".join(set(x.str.cat(sep=";").split(";")))).reset_index()
    second_filter_2 = \
        df.groupby(["country_iso", "decision_date", "jurisdiction", "policy_name"])[
            "sector"].apply(lambda x: ";".join(set(x.str.cat(sep=";").split(";")))).reset_index()
    second_filter_3 = \
        df.groupby(["country_iso", "decision_date", "jurisdiction", "policy_name"])[
            "policy_description"].apply(lambda x: "\n".join(set(x.str.cat(sep="¥¥").split("¥¥")))).reset_index()
    second_filter_4 = \
        df.groupby(["country_iso", "decision_date", "jurisdiction", "policy_name"])[
            "policy_type"].apply(lambda x: ";".join(set(x.str.cat(sep=";").split(";")))).reset_index()
    second_filter_5 = \
        df.groupby(["country_iso", "decision_date", "jurisdiction", "policy_name"])[
            "policy_objective"].apply(lambda x: ";".join(set(x.str.cat(sep=";").split(";")))).reset_index()
    # second_filter_6 = df.groupby(["Country ISO", "decision_date", "jurisdiction"])[
    #     "policy_name"].apply(lambda x: ";".join(set(x.str.cat(sep=";").split(";")))).reset_index()
    second_filter["sector"] = second_filter_2["sector"]
    second_filter["policy_description"] = second_filter_3["policy_description"]
    second_filter["policy_type"] = second_filter_4["policy_type"]
    second_filter["policy_objective"] = second_filter_5["policy_objective"]
    # second_filter["policy_name"] = second_filter_6["policy_name"]
    # print(df.info())

    # drop_duplicates by "Country ISO", "Date of decision", "jurisdiction", "policy_name"
    third_filter = df.drop_duplicates(
        subset=["country_iso", "decision_date", "jurisdiction", "policy_name"],
        keep="first")

    fourth_filter = third_filter.drop(
        ["policy_instrument", "sector", "policy_description", "policy_type", "policy_objective"], axis=1)

    # merge by "Country ISO", "Date of decision", "jurisdiction", "policy_name"
    result_filter = pd.merge(second_filter, fourth_filter,
                             on=["country_iso", "decision_date", "jurisdiction", "policy_name"])
    
    result_filter.to_excel('policy_db_iea_cp_cclw_update/cp_dedup_result.xlsx', index=False)

    with open("policy_db_iea_cp_cclw_update/dup_statistic.txt", 'a') as f:
        f.write("Climate Policy Raw: " + str(len(df)) + '\n')
        f.write("Climate Policy After cp_dedup.py: " + str(len(result_filter)) + '\n')
        f.write("Climate Policy first dup: " + str(len(df) - len(result_filter)) + '\n')
        f.write('\n')


if __name__ == '__main__':
    pd.set_option('display.max_columns', 4)
    cp_df = get_data()
    print(len(cp_df))
    data_process(cp_df)
