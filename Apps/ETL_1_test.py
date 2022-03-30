import pytest

from chispa import assert_df_equality
from pyspark.sql import SparkSession
from Utilities.File_Functions import ( 
    setDropColumnDF,
    setFileredDF,
    setJoinDF,
    setRenameColumnsDF,
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("ETL1").getOrCreate()

"""
All tests will work with same prinncipal of comparing dataframes with assert_df_equality method from chispa:
one prepeared in test fuction and one after run of function from Utilities library

One sample test prepered for  Utilities.File_Functions.setDropColumnDF

"""

def test_setDropColumnDF(spark):
    arrInputData = [
        ("row11", "row12", "row13", "row14"),
        (("row21", "row22", "row23", "row24")),
    ]
    arrInputSchema = ["col1", "col2", "col3", "col4"]
    arrOutputData = [("row11", "row12"), ("row21", "row22")]
    arrOutputSchema = ["col1", "col2"]
    arrTaskSchema = ["col3", "col4"]
    inDF = spark.createDataFrame(arrInputData, arrInputSchema)
    outDF = spark.createDataFrame(arrOutputData, arrOutputSchema)

    assert_df_equality(
        setDropColumnDF(inDF, arrTaskSchema), outDF
    )

