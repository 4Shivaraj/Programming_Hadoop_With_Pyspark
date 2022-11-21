"""
    @Author: Shivaraj
    @Date: 18-10-2022
    @Last Modified by: Shivaraj
    @Last Modified date: 18-10-2022
    @Title: 1. Use the static data available with BridgeLabz - Some GBs of data
            2. Store the data on on-premises DB
            3. Preprocess the data (Use Python + Spark)
            4. Finding users with lowest & highest numbers of average hours
            5. Finding users with the highest numbers of times late comings & idle hours
            6. Store results on BLOB Storage
"""
import os
import sys
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from pyspark.sql import *
from pyspark.sql.functions import date_format, col, lit, to_timestamp, desc, row_number
from data_log import get_logger

lg = get_logger('Pyspark user log','data_log.log')


class OnPremiseToAzureBlobStorage:
    def __init__(self, my_dict: dict):
        self.csv_location = my_dict.get('csv_location')
        self.output_location = my_dict.get('output_location')
        self.output_location1 = my_dict.get('output_location1')
        self.output_location2 = my_dict.get('output_location2')
        self.output_location3 = my_dict.get('output_location3')
        self.connect_string = my_dict.get('connect_string')
        self.container = my_dict.get('container')
        self.blob_name = my_dict.get('blob_name')
        self.blob_name1 = my_dict.get('blob_name1')
        self.blob_name2 = my_dict.get('blob_name2')
        self.blob_name3 = my_dict.get('blob_name3')
        self.df1 = my_dict.get('df1')


    def save_output(self):
        """
        Description:
            This function is used to perform merge the csv files and save it to the output location.
        Parameter:
            None
        Return:
            Returns df1 is the merged output.
        """
        try:
            print(df1.describe().show())
            df1.coalesce(1).write.mode('overwrite').csv(self.output_location)
            print(f'Saved to the {self.output_location} location successfully')
            return df1
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")
            spark.stop()

    def max_and_min_hours(self):
        """
        Description:
            This function is used to find the maximum and minimum hours the user have worked.
        Parameter:
            None
        Return:
            Returns df1 is the merged output.
        """
        try:
            df = self.save_output()
            print("printing all the selected table")
            # Using column string to select column details
            log_data = df.select('user_name', 'DateTime', 'keyboard', 'mouse')
            print(log_data.show())

            log_data1 = log_data.groupby('user_name').count()  # DataFrame[user_name: string, count: bigint]
            print(log_data1)
            # withColumn: syntax:: withColumn("new column we want to add", func('input')
            log_data2 = log_data1.withColumn('working_hours', (col('count') * 5) / 60).sort('working_hours',
                                                                                            ascending=False)
            print(log_data2.show())
            pd_data_frame = log_data2.toPandas()
            print(pd_data_frame.head())
            print(
                f'The maximum working hour the user {pd_data_frame["user_name"][0]} worked is {pd_data_frame["working_hours"][0]} hours')
            print(
                f'The minimum working hour the user {pd_data_frame["user_name"].iloc[-1]} worked is {pd_data_frame["working_hours"].iloc[-1]} hours')
            log_data2.coalesce(1).write.mode('overwrite').csv(self.output_location1)
            print(f'Saved to the {self.output_location1} location successfully')
            self.upload_blob_to_azure_blob_storage(self.output_location1, self.blob_name1)
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")
            spark.stop()

    def max_min_idle_hours(self):
        """
        Description:
            This function is used to find the maximum and minimum idle hours.
        Parameter:
            None
        Return:
            Returns None.
        """
        try:
            df = self.save_output()
            print("Printing all the selected table")
            log_data = df.select('user_name', 'DateTime', 'keyboard', 'mouse')
            print(log_data.show())
            working_user_idle1 = log_data.filter((log_data['keyboard'] == 0.0) & (log_data['mouse'] == 0.0))
            working_user_idle1.show()
            working_user_idle2 = working_user_idle1.groupby('user_name').count()
            working_user_idle3 = working_user_idle2.withColumn('idle_working_hours', (col('count') * 5) / 60).sort(
                'idle_working_hours')
            print(working_user_idle3.show())
            pd_data_frame = working_user_idle3.toPandas()
            pd_data_frame.head()
            print(
                f'The minimum hour the user {pd_data_frame["user_name"][0]} in-active is {pd_data_frame["idle_working_hours"][0]} hours')
            print(
                f'The maximum hour the user {pd_data_frame["user_name"].iloc[-1]} in-active is {pd_data_frame["idle_working_hours"].iloc[-1]} hours')
            working_user_idle3.coalesce(1).write.mode('append').csv(self.output_location2)
            print(f'Saved to the {self.output_location2} location successfully')
            self.upload_blob_to_azure_blob_storage(self.output_location2, self.blob_name2)
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")
            spark.stop()

    def max_and_min_late_commers(self):
        """
        Description:
            This function is used to find the max and min late commers.
        Parameter:
            None
        Return:
            Returns None.
        """
        try:
            df = self.save_output()
            print(df.describe().show())
            df.printSchema()

            df1 = df.select('DateTime', 'user_name')
            print(df1.printSchema())
            print(df1.show())

            df1_data = df1.withColumn('time', date_format('DateTime', 'HH:mm')).sort('time')
            print(df1_data.show())
            print(df1_data.printSchema())

            w2 = Window.partitionBy('user_name').orderBy(col('user_name'))
            df2 = df1_data.withColumn('row', row_number().over(w2))
            print(df2.show(1000))

            df3 = df2.filter(col('row') == 1).drop('row').sort('time')
            print(df3.show())

            df1_date = df3.first()['DateTime']
            print(df1_date)

            # lit: Constant or literal value as new column to the dataframe
            df3_wip = df3.withColumn('IntialDateTime', lit(df1_date))
            df3_wip.show()
            df4 = df3_wip.withColumn('time_intial', date_format('IntialDateTime', 'HH:mm'))
            print(df4.show())
            print(df4.printSchema())

            df2_01 = df4.withColumn('from_time', to_timestamp(col('time_intial'))).withColumn('end_time', to_timestamp(
                col('time'))).withColumn('DiffInSeconds', (col('end_time')).cast('long') - col('from_time').cast('long'))
            print(df2_01.show())
            print(df2_01.printSchema())

            df2_02 = df2_01.withColumn('LateComingCount', (col('DiffInSeconds') / (60 * 5))).sort(desc('DiffInSeconds'))
            late_commers = df2_02.select('user_name', 'LateComingCount')
            print(late_commers.show())

            df_pd = late_commers.toPandas()
            print(df_pd.head())

            print(
                f'The username with lowest number of late comings is: {df_pd["user_name"].iloc[-1]} with count: {df_pd["LateComingCount"].iloc[-1]}')
            print(
                f'The username with highest number of late comings is: {df_pd["user_name"].iloc[0]} with count: {df_pd["LateComingCount"].iloc[0]}')
            late_commers.coalesce(1).coalesce(1).write.mode('append').csv(self.output_location3)
            print(f'Saved to the {self.output_location3} location successfully')
            self.upload_blob_to_azure_blob_storage(self.output_location3, self.blob_name3)
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")
            spark.stop()

    def upload_blob_to_azure_blob_storage(self, output_location, blob_name):
        """
        Description:
            This function is used to upload blob to azure blob storage.
        Parameter:
            output_location: output location needs to be checked.
            blob_name: blob_name needs to be checked.
        Return:
            Returns None
        """
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.connect_string)
            blob_client = blob_service_client.get_blob_client(container=self.container, blob=blob_name)
            folder_name = output_location
            file = os.listdir(folder_name)[2]
            with open(folder_name+file, 'rb') as blob:
                blob_client.upload_blob(data = blob, overwrite=True)
            print('File Successfully Uploaded')
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")
            spark.stop()


if __name__ == '__main__':
    try:
        load_dotenv()
        csv_location = 'D:/DataEngg/CFP_Assignment/cpu_logs_db/*.csv'
        output_location = 'D:/DataEngg/CFP_Assignment/cpu_logs_db/output/'
        output_location1 = 'D:/DataEngg/CFP_Assignment/cpu_logs_db/output1/'
        output_location2 = 'D:/DataEngg/CFP_Assignment/cpu_logs_db/output2/'
        output_location3 = 'D:/DataEngg/CFP_Assignment/cpu_logs_db/output3/'
        connect_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        container = 'csvs-to-azure-blob'
        blob_name = 'output_dest.csv'
        blob_name1 = 'output_dest1.csv'
        blob_name2 = 'output_dest2.csv'
        blob_name3 = 'output_dest3.csv'
        spark = SparkSession.builder.appName('Merge File And Store To Azure Blog Sorage').master(
            'local[*]').getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel('ERROR')
        df1 = spark.read.format('csv') \
            .option('header', True) \
            .option('inferSchema', True) \
            .option('path', csv_location).load()
        my_dict = {'csv_location': csv_location, 'output_location': output_location, 'output_location1': output_location1,
                   'output_location2': output_location2, 'output_location3': output_location3, 'connect_string': connect_string,
                   'container': container, 'blob_name': blob_name,'blob_name1': blob_name1,'blob_name2': blob_name2,
                   'blob_name3': blob_name3,'df1': df1}

        c_obj = OnPremiseToAzureBlobStorage(my_dict)
        c_obj.save_output()
        c_obj.upload_blob_to_azure_blob_storage(output_location, blob_name)
        c_obj.max_and_min_hours()
        c_obj.max_min_idle_hours()
        c_obj.max_and_min_late_commers()
    except Exception:
        exception_type, _, exception_traceback = sys.exc_info()
        line_number = exception_traceback.tb_lineno
        lg.exception(
            f"Exception type : {exception_type} \nError on line number : {line_number}")
        spark.stop()








