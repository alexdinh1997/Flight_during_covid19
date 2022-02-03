from typing import cast
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

from datetime import datetime,timedelta
class clean:
    @staticmethod
    def clean_fact_table(fact):


        string_to_date= udf(lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
            """
            Function: Date conversion for a date string col
            Param: date Columns
            Output: a date columns with type of date
            """
        fact = fact.withColumn('Flight_date', string_to_date(col('FL_DATE')))\
                    .withColumnRenamed("MKT_UNIQUE_CARRIER","c_airline")\
                    .withColumn("Flight_num",col("MKT_CARRIER_FL_NUM").cast("int"))\
                    .withColumnRenamed("MKT_CARRIER_FL_NUM","Flight_num").\
                    .withColumn("")
                    .drop("FL_DATE")
                    .withColumn("CRS_arr_time",lpad(col("CRS_ARR_TIME"),4,'0'))\
                    .withColumn("CRS_dep_time",lpad(col("CRS_DEP_TIME"),4,'0'))\
                    .withColumn("Dep_time",lpad(col("DEP_TIME"),6,'0'))\
                    .withColumn("Arr_time",lpad(col("ARR_TIME"),6,'0')).drop("CRS_DEP_TIME_1")\
                    .withColumn("CRS_dep_time",date_format(to_timestamp("CRS_dep_time",'HHmm'),"HH:mm"))\
                    .withColumn("CRS_arr_time",date_format(to_timestamp("CRS_arr_time",'HHmm'),"HH:mm"))\
                    .withColumn("Dep_time",date_format(to_timestamp("Dep_time",'HHmm.0'),"HH:mm"))\
                    .withColumn("Arr_time",date_format(to_timestamp("Arr_time",'HHmm.0'),"HH:mm")).show()

        return fact.select(col("Flight_date"),col())
