# PySpark App description
App crated to perform ETL operation on two .csv files:
**List of actions**
- setts up spark session
- reads cvs files
- manipulate files by droping columns
- manipulate files by filtering rows
- manipulate files by joining (inner join) files to single data set
- manipulating data set of joined files by renaming set of columns
- writes combined data set to single file in path "/client_data/output.csv".

# Installation
- App is using Spark: please falow linked tutorial to instal all reguired sofware on WindowsOS
     https://sparkbyexamples.com/pyspark/install-pyspark-for-python/
- App is using required libraries: please instal using comand 

** 
    pip install -r requirements.txt 
** 
# Arguments description and run command
- strCSVPath1 = path to data set 1 (./DataSets/dataset_one.csv)
- strCSVPath2 = path to data set 2 (./DataSets/dataset_two.csv )
- arrCoutryList = list of countries to be filter out from data set 
- **[Attention]** in case of two ore more words in Country name (like United Kindom) please use "_" instead of space like (United_Kindom)
- Please use sample bash comand to run the app 
 
 ** 
    python ETL_1.py dataset_one.csv dataset_two.csv Netherlands United_Kingdom 
 ** 
