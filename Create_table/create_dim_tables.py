import pandas as pd
import pyspark.sql.*
import re

 class create_dim:

    # create session
    spark = SparkSession \
        .builder \
        .appName("Create schema flight-during-covid19 data") \
        .getOrCreate()

    # extract from metadata .txt files: airlines and CANCELLATION_CODE

    @staticmethod
    def create_airline_table(path1,path2):
        """
        Function: Generate and create airlines table code
        param:
            - path1: txt file
            - path2: dataset file
        output: airline.csv file stored in data folder
        """
        df = spark.read.csv(path2, header=True)
        df2 = df.select("MKT_UNIQUE_CARRIER","MKT_CARRIER_FL_NUM","ORIGIN","DEST","TAIL_NUM").dropDuplicates()
        with open(path1) as f:
            content = f.readlines()
            content = [x.strip() for x in content]
            airline = content[10:20]
            splitted_airline = [c.split(":") for c in airline]
            c_airline = [x[0].replace("'","").strip() for x in splitted_airline]
            airline_name = [x[1].replace("'","").strip() for x in splitted_airline]
            airline_df = spark.createDataFrame(zip(c_airline, airline_name), schema=['c_airline', 'airline_name'])
        return airline_df = airline_df.join(df2,airline_df.c_airline == df2.MKT_UNIQUE_CARRIER,"inner")\
                        .drop("MKT_UNIQUE_CARRIER","ORIGIN","DEST")


    @staticmethod
    def create_cancelation_table(path):
        """
        Function: Generate and create Cancelation_code table:
        param: Path of datafile
        input: .txt file
        output: cancel.csv file stored in data folder
        """
        with open(path) as f:
            content = f.readlines()
            content = [x.strip() for x in content]
            cancel = [re.search('\(([^)]+)', content[49]).group(1)][0].split(",")
            splitted_cancel = [c.split(":") for c in cancel]
            c_cancel = [x[0].replace("'","").strip() for x in splitted_cancel]
            cancel_des= [x[1].replace("'","").strip() for x in splitted_cancel]
            cancel_df = pd.DataFrame({"c_cancel" : c_cancel, "cancel_des": cancel_des})
            return cancel_df.to_csv("data/airline.csv")

    @staticmethod
    def create_port_loc_table():
        """
        Function:
        param:
        input:
        output:
        """
        df = spark.read.csv(path, header=True)
        for column in df.columns:
            df = df.withColumnRenamed(column, column.lower())
        port_loc_df = df.select('origin', 'origin_city_name', 'origin_state_abr').dropDuplicates()
        port_loc_df = port_loc_df.withColumn('origin_city_name', split(port_loc_df['origin_city_name'], ',').getItem(0))
        return port_loc_df.toPandas().to_csv('data/port_loc.csv', index=False)

    @staticmethod
    def create_distance_group():
        """
        """
        data = []
        for i in range(26):
            data.append([i, "{} <= distance < {}".format(i * 250, (i + 1) * 250)])

        df = pd.DataFrame(data=data, columns=['distance_group', 'distance_range(miles)'])
        df.to_csv('data/distance_group.csv', index=False)

    @staticmethod
    def create_states_df(path):
        """
        Function:

        """
        df = spark.read.csv(path, header=True)
        for column in df.columns:
            df = df.withColumnRenamed(column, column.lower())
        state_df = df.select('origin_state_abr', 'origin_state_nm').dropDuplicates()
        return state_df.toPandas().to_csv('data/states.csv', index=False)


    @staticmethod
    def create_delay_group():
        """
        function
        """
        data = []
        for i in range(-1,188):
            if i == -1:
                data.append([-1,"Early"])
            elif i == 0:
                data.append([0,"On Time"])
            else:
                data.append([i, "{} <= delay time < {}".format(i * 15, (i + 1) * 15)])


        df = pd.DataFrame(data=data, columns=['delay_group', 'delay_time_range(minutes)'])
        df.to_csv('data/delay_group.csv', index=False)
