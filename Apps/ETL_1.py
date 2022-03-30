import sys

from Utilities.File_Functions import *

def ETL_1(strCSVPath1: str, strCSVPath2: str, arrCoutryList):

    strAppName = "ETL1"
    strOutPath = "client_data"
    strOutFile = "output"
    #strCSVPath1 = "dataset_one.csv" 
    #strCSVPath2 = "dataset_two.csv" 
    #arrCoutryList = ['Netherlands', 'United Kingdom']

    logging.basicConfig(level=logging.INFO)

    spark = setSparkSession(strAppName)
    df1 = readCSVtoDF(f"./DataSets/{strCSVPath1}", spark)
    df2 = readCSVtoDF(f"./DataSets/{strCSVPath2}", spark)
    # removing columns and filtering data for smaller datasets and easier join
    df1 = setDropColumnDF(df1, ["first_name", "last_name"])
    df2 = setDropColumnDF(df2, ["cc_n"])
    df1 = setFileredDF(df1, {"country": arrCoutryList})
    df = setJoinDF(df1, df2, "id",'inner')
    dictRenameColumnValue = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type",
        }
    df = setRenameColumnsDF(df, dictRenameColumnValue)
    writeCSVfromDF(df,strOutPath,strOutFile,strAppName)

#python ETL_1.py dataset_one.csv dataset_two.csv Netherlands United_Kingdom

if __name__ == "__main__":
    
    ETL_1(sys.argv[1],sys.argv[2],sys.argv[3:])