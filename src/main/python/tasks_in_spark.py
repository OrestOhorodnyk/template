
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, year, avg, col, round

gps_country_file_path = '/home/orest/Documents/TRENING/template/src/main/resources/gps_country_city.csv'
meteo_data_file_path = '/home/orest/Documents/TRENING/template/src/main/resources/meteo_data.json.gz'

spark = SparkSession.builder.master("local[*]").appName("Spark_app").getOrCreate()
gps_country_df = spark.read.csv(path=gps_country_file_path, header=True)
gps_country_df.printSchema()
gps_country_df.show()

#  - step 1:  Load the file so it is available for processing (preferably as a big-data Spark RDD, DataFrame or alike).
#             Display short sample of the data.
meteo_data_df = spark.read.json(meteo_data_file_path)
meteo_data_df = meteo_data_df.select(explode(meteo_data_df.data))
meteo_data_df = meteo_data_df.select(
    meteo_data_df.col.date.alias('date'),
    meteo_data_df.col.lat.alias('lat'),
    meteo_data_df.col.long.alias('long'),
    meteo_data_df.col.tC.alias('tC')
)
meteo_data_df.show()
meteo_data_df.printSchema()
# - step 2:  Find average temperature by year (you may plot  a chart) and answer if the data shows there is a global
#            warming visible or not;
meteo_data_df.groupBy(
    year(meteo_data_df.date).alias('year')
).agg(round(avg(meteo_data_df.tC), 1)).orderBy(col('year')).show()
# print(df.count())
print(type(meteo_data_df))

