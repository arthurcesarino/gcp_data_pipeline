from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import current_timestamp, col, struct, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType


# set default location for warehouse
warehouse_location = abspath('spark-warehouse')

# set curated zone bucket address
curated_zone_address = 'gs://dp-curated-zone/f1_results'

# main spark program
if __name__ == '__main__':


    # init session
    spark = SparkSession \
            .builder \
            .appName('etl-f1-py') \
            .config('spark.sql.warehouse.dir', warehouse_location) \
            .enableHiveSupport() \
            .getOrCreate()


    # show configured parameters
    print(SparkConf().getAll())


    # set log level
    spark.sparkContext.setLogLevel('INFO')

    # set dynamic input file [hard-coded]
    # can be changed for input parameters [spark-submit]
    get_drivers_file = 'gs://dp-processing-zone/files/drivers/*.json'
    get_results_file = 'gs://dp-processing-zone/files/results/*.json'


    # drivers.json schema
    # name field has a struct json, so we need to have one structtype only for the name
    name_schema = StructType(fields=[
        StructField('forename', StringType(), True),
        StructField('surname', StringType(), True)
    ])

    drivers_schema = StructType(fields=[
        StructField('driverId', IntegerType(), False),
        StructField('driverRef', StringType(), True),
        StructField('number', IntegerType(), True),
        StructField('code', StringType(), True),
        StructField('name', name_schema),
        StructField('dob', DateType(), True),
        StructField('nationality', StringType(), True),
        StructField('url', StringType(), True)
    ])


    # results schema
    results_schema = StructType(fields=[
        StructField('resultId', IntegerType(), False),
        StructField('raceId', IntegerType(), True),
        StructField('driverId', IntegerType(), True),
        StructField('constructorId', IntegerType(), True),
        StructField('number', IntegerType(), True),
        StructField('grid', IntegerType(), True),
        StructField('position', IntegerType(), True),
        StructField('positionText', StringType(), True),
        StructField('positionOrder', IntegerType(), True),
        StructField('points', FloatType(), True),
        StructField('laps', IntegerType(), True),
        StructField('time', StringType(), True),
        StructField('milliseconds', IntegerType(), True),
        StructField('fastestLap', IntegerType(), True),
        StructField('rank', IntegerType(), True),
        StructField('fastestLapTime', StringType(), True),
        StructField('fastestLapSpeed', FloatType(), True),
        StructField('statusId', StringType(), True)
    ])

    # read drivers data
    df_driver = spark.read \
                .schema(drivers_schema) \
                .json(get_drivers_file)


    # read ressults data
    df_results = spark.read \
                .schema(results_schema) \
                .json(get_results_file)



    # select results columns
    df_results_selected = df_results.select(col('resultId'),
                                            col('driverId'),
                                            col('grid'),
                                            col('position'),
                                            col('points'),
                                            col('rank'))  

    # display dataframes
    df_driver.show()
    df_results.show()       


    # create renamed df whith columns
    df_driver_renamed = df_driver.withColumnRenamed('driverId', 'driver_id') \
                                 .withColumnRenamed('driverRef', 'driver_ref') \
                                 .withColumn('ingestion_date', current_timestamp()) \
                                 .withColumn('name', concat(col('name.forename'), lit(" "), col('name.surname')))
    

    df_results_renamed = df_results_selected.withColumnRenamed('resultId', 'result_id') \
                                            .withColumnRenamed('driverId', 'driver_id') \
                                            .withColumn('ingestion_date', current_timestamp())

    # drop unwanted columnns
    df_driver_final = df_driver_renamed.drop(col('url'))


    # create temp view to access the spark sql API
    df_driver_final.createOrReplaceTempView('drivers')
    df_results_renamed.createOrReplaceTempView('results')


    # join data frames and group by driver ID
    df_join = spark.sql(
        """
        SELECT  drivers.driver_id,
                drivers.name,
                avg(results.points) as avg_points,
                avg(results.rank) as avg_rank
        FROM drivers 
        INNER JOIN results
        ON drivers.driver_id = results.driver_id
        GROUP BY drivers.driver_id, drivers.name
        ORDER BY avg_points DESC
        """
    )

    # show & counbt dnv
    df_join.explain()
    df_join.count()

    df_join.show()


    # write dataframe into curated zone inside gcs
    df_join.write.format('parquet').mode('overwrite').save(curated_zone_address)

    # stop spark application
    spark.stop()

