import sys

# adding Utilities to the system path
sys.path.append("..")
from Utilities.File_Functions import *

def ETL_1(str_CSV_Path_1: str, str_CSV_Path_2: str, arr_Coutry_List):

    str_App_Name = "ETL1"
    str_Out_Path = "client_data"
    str_Out_File = "output"
    #str_CSV_Path_1 = "dataset_one.csv" 
    #str_CSV_Path_2 = "dataset_two.csv" 
    #arr_Coutry_List = ['Netherlands', 'United_Kingdom']

    logging.basicConfig(level=logging.INFO)

    spark = set_Spark_Session(str_App_Name)
    df1 = read_CSV_to_DF(f"../DataSets/{str_CSV_Path_1}", spark)
    df2 = read_CSV_to_DF(f"../DataSets/{str_CSV_Path_2}", spark)
    # removing columns and filtering data for smaller datasets and easier join
    df1 = set_Drop_Column_DF(df1, ["first_name", "last_name"])
    df2 = set_Drop_Column_DF(df2, ["cc_n"])
    df1 = set_Filered_DF(df1, {"country": arr_Coutry_List})
    df = set_Join_DF(df1, df2, "id",'inner')
    dict_Rename_Column_Value = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type",
        }
    df = set_Rename_Columns_DF(df, dict_Rename_Column_Value)
    write_CSV_from_DF(df,str_Out_Path,str_Out_File,str_App_Name)

#python ETL_1.py dataset_one.csv dataset_two.csv Netherlands United_Kingdom

if __name__ == "__main__":
    
    ETL_1(sys.argv[1],sys.argv[2],sys.argv[3:])
