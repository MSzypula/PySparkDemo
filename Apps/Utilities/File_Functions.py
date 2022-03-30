import logging

from distutils import file_util
from os import  listdir, remove, rmdir
from typing import  List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col as colfunc


#===================================setSparkSession===========================
def setSparkSession(strAppName: str):
    spark = SparkSession.builder.appName(strAppName).getOrCreate()  
    logging.info(f'F:setSparkSession - SparkSession: Successfully SET')
    return spark

#===================================readCSV===========================
def readCSVtoDF(strPath: str, spark: SparkSession) -> DataFrame:
    """AI is creating summary for readCSVtoDF

    Args:
        strPath (str): [description]
        spark (SparkSession): [description]

    Returns:
        DataFrame: [description]
    """
    logging.info(f'F:readCSV - File "{strPath}" has been read to DataFrame')
    return spark.read.csv(strPath,header=True)



#===================================setDropColumnDF===========================
def setDropColumnDF(df: DataFrame, arrColumns: List) -> DataFrame:
    """AI is creating summary for setDropColumnDF

    Args:
        df (DataFrame): [description]
        arrColumns (List): [description]

    Returns:
        DataFrame: [description]
    """
    
    logging.info(f"F:setDropColumnDF - Dropping columns: {arrColumns} from DataFrame" )
    return df.drop(*arrColumns)

    return spark.read.csv(strPath,header=True)


#===================================setFileredDF===========================
def setFileredDF(df: DataFrame, dictColumnValue) -> DataFrame:
    """AI is creating summary for setFileredDF

    Args:
        df (DataFrame): [description]
        dictColumnValue ([type]): [description]

    Returns:
        DataFrame: [description]
    """
    

    for col, values in dictColumnValue.items():
        if values is None:
            pass
        else:
            #Standardising values
            for i in range(len(values)):
                if values[i].find("_") > 0:
                    values[i]=values[i].replace("_", " ")

            logging.info(f'F:setFileredDF - Filtering "{col} = {values}" from DataFrame')
            df = df.filter(colfunc(f"{col}").isin(values))
    return df



#===================================setJoinDF===========================
def setJoinDF(df1: DataFrame, df2: DataFrame, strKey: str, StrOption: str = 'inner') -> DataFrame:
    """AI is creating summary for setJoinDF

    Args:
        df1 (DataFrame): [description]
        df2 (DataFrame): [description]
        strKey (str): [description]
        StrOption (str, optional): [description]. Defaults to 'inner'.

    Returns:
        DataFrame: [description]
    """

    logging.info(f"F:setJoinDF - Performing {StrOption} join on: {strKey}")
    if StrOption is not None:
        return df1.join(df2, strKey,how=StrOption)
    else:
        return df1.join(df2, strKey)

#===================================setRenameColumnsDF===========================
def setRenameColumnsDF(df: DataFrame, dictColumnValue) -> DataFrame:
    """AI is creating summary for setRenameColumnsDF

    Args:
        df (DataFrame): [description]
        dictColumnValue ([type]): [description]

    Returns:
        DataFrame: [description]
    """

    for col, values in dictColumnValue.items():
        logging.info(f"F:setRenameColumnsDF - Renaming column: {col} to: {values} in DataFrame")
        df = df.withColumnRenamed(f"{col}", f"{values}")
    return df



#===================================writeCSV===========================
def writeCSVfromDF(df: DataFrame, strPath: str, strFileName: str, strAppName: str) :
    """AI is creating summary for writeCSVfromDF

    Args:
        df (DataFrame): [description]
        strPath (str): [description]
        strFileName (str): [description]
        strAppName (str): [description]
    """
    #Write one all partiotions to one csv file
    strTempPath = f"./DataSets/output_datasets/{strAppName}"
    df.coalesce(1).write.csv(f"{strTempPath}",mode='overwrite',header=True)
    
    #Get csv file name
    for file in listdir(f"{strTempPath}"):
        if file.endswith(".csv"):
            strSrcFile = f"{strTempPath}/{file}"
    
    #Copy temp file to required path and name
    file_util.copy_file(f"{strSrcFile}",f"./{strPath}/{strFileName}.csv",update=True,link='hard')   
    
    #Clean up temp folder and files
    for file in listdir(f"{strTempPath}"):
        remove(f"{strTempPath}/{file}")
    rmdir(f"{strTempPath}")

    logging.info(f'F:writeCSV - Writing DataFrame as: "{strPath}/{strFileName}.csv"')
