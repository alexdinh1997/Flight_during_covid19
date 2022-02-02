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

        return fact.select(col("Flight_date"),col())
