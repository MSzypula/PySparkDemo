import logging

from distutils import file_util
from os import  listdir, remove, rmdir
from typing import  List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col as colfunc


#===================================set_Spark_Session===========================
def set_Spark_Session(str_App_Name: str):
    spark = SparkSession.builder.appName(str_App_Name).getOrCreate()  
    logging.info(f'F:set_Spark_Session - SparkSession: Successfully SET')
    return spark

#===================================read_CSV_to_DF===========================
def read_CSV_to_DF(str_Path: str, spark: SparkSession) -> DataFrame:

    """Function read_CSV_to_DF reades provided in argument file to dataframe

    Args:
        str_Path (str): [Patch to file]
        spark (SparkSession): 

    Returns:
        DataFrame: 
    """
    logging.info(f'F:read_CSV_to_DF - File "{str_Path}" has been read to DataFrame')
    return spark.read.csv(str_Path,header=True)



#===================================set_Drop_Column_DF===========================
def set_Drop_Column_DF(df: DataFrame, arr_Columns: List) -> DataFrame:

    """Function set_Drop_Column_DF deletes from datframe column provided in argument list

    Args:
        df (DataFrame): 
        arr_Columns (List): [list of columns]

    Returns:
        DataFrame: 
    """
    logging.info(f"F:set_Drop_Column_DF - Dropping columns: {arr_Columns} from DataFrame" )
    return df.drop(*arr_Columns)




#===================================set_Filered_DF===========================
def set_Filered_DF(df: DataFrame, dict_Column_Value) -> DataFrame:

    """Function set_Filered_DF is filtering dataframe with provided values dictonary

    Args:
        df (DataFrame): 
        dict_Column_Value ([type]): [dictonary of values to filer out - columnName : list od values]

    Returns:
        DataFrame: 
    """
    

    for col, values in dict_Column_Value.items():
        if values is None:
            pass
        else:
            #Standardising values
            for i in range(len(values)):
                if values[i].find("_") > 0:
                    values[i]=values[i].replace("_", " ")

            logging.info(f'F:set_Filered_DF - Filtering "{col} = {values}" from DataFrame')
            df = df.filter(colfunc(f"{col}").isin(values))
    return df



#===================================set_Join_DF===========================
def set_Join_DF(df1: DataFrame, df2: DataFrame, str_Key: str, str_Option: str = 'inner') -> DataFrame:

    """Function set_Join_DF is joining two dataframes by provided argument key and option

    Args:
        df1 (DataFrame): [first data set]
        df2 (DataFrame): [second data set]
        str_Key (str): [key of join]
        str_Option (str, optional): [type of join]. Defaults to 'inner'.

    Returns:
        DataFrame: 
    """

    logging.info(f"F:set_Join_DF - Performing {str_Option} join on: {str_Key}")
    return df1.join(df2, str_Key,how=str_Option)
   

#===================================set_Rename_Columns_DF===========================
def set_Rename_Columns_DF(df: DataFrame, dict_Column_Value) -> DataFrame:

    """Function set_Rename_Columns_DF renames columns in dataframe on provided dictonary list

    Args:
        df (DataFrame): 
        dict_Column_Value ([type]): [dictornary of colunm and new value for given columnn]

    Returns:
        DataFrame: 
    """

    for col, values in dict_Column_Value.items():
        logging.info(f"F:set_Rename_Columns_DF - Renaming column: {col} to: {values} in DataFrame")
        df = df.withColumnRenamed(f"{col}", f"{values}")
    return df



#===================================writeCSV===========================
def write_CSV_from_DF(df: DataFrame, str_Path: str, str_File_Name: str, str_App_Name: str):

    """AI is creating summary for write_CSV_from_DF
    Args:
        df (DataFrame): 
        str_Path (str): [output path]
        str_File_Name (str): [output file name]
        str_App_Name (str): [app name]
    """
    #Write one all partiotions to one csv file
    str_Temp_Path = f"../DataSets/output_datasets/{str_App_Name}"
    df.coalesce(1).write.csv(f"{str_Temp_Path}",mode='overwrite',header=True)
    
    #Get csv file name
    for file in listdir(f"{str_Temp_Path}"):
        if file.endswith(".csv"):
            str_Src_File = f"{str_Temp_Path}/{file}"
    
    #Copy temp file to required path and name
    file_util.copy_file(f"{str_Src_File}",f"../{str_Path}/{str_File_Name}.csv",update=True,link='hard')   
    
    #Clean up temp folder and files
    for file in listdir(f"{str_Temp_Path}"):
        remove(f"{str_Temp_Path}/{file}")
    rmdir(f"{str_Temp_Path}")

    logging.info(f'F:writeCSV - Writing DataFrame as: "{str_Path}/{str_File_Name}.csv"')