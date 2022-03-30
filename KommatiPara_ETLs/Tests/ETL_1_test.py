import pytest
import sys

from chispa import assert_df_equality
from pyspark.sql import SparkSession

sys.path.append("..")
from Utilities.File_Functions import ( 
    set_Drop_Column_DF,
    set_Filered_DF,
    set_Join_DF,
    set_Rename_Columns_DF,
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("ETL1").getOrCreate()

"""
All tests will work with same prinncipal of comparing dataframes with assert_df_equality method from chispa:
one prepeared in test fuction and one after run of function from Utilities library

One sample test prepered for  Utilities.File_Functions.set_Drop_Column_DF

"""

def test_set_Drop_Column_DF(spark):
    arr_Input_Data = [
        ("row11", "row12", "row13", "row14"),
        (("row21", "row22", "row23", "row24")),
    ]
    arr_Input_Schema = ["col1", "col2", "col3", "col4"]
    arr_Output_Data = [("row11", "row12"), ("row21", "row22")]
    arr_Output_Schema = ["col1", "col2"]
    arr_Task_Schema = ["col3", "col4"]
    inDF = spark.createDataFrame(arr_Input_Data, arr_Input_Schema)
    outDF = spark.createDataFrame(arr_Output_Data, arr_Output_Schema)

    assert_df_equality(
        set_Drop_Column_DF(inDF, arr_Task_Schema), outDF
    )

