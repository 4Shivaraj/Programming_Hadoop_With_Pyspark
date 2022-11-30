"""
    @Author: Shivaraj
    @Date: 22-10-2022
    @Last Modified by: Shivaraj
    @Last Modified date: 22-10-2022
    @Title:
"""

import os
import sys
from dotenv import load_dotenv
from pyspark.sql import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from azure.storage.blob import BlobServiceClient
from data_log import get_logger

lg = get_logger('Pyspark twitter data log','data_log.log')

class PreprocessingLoadToBlobStorage:
    def __init__(self, my_dict: dict):
        self.spark = my_dict.get('spark')
        self.connect_string = my_dict.get('connect_string')
        self.container = my_dict.get('container')
        self.blob_name = my_dict.get('blob_name')
        self.input_csv1 = my_dict.get('input_csv1')
        self.input_csv2 = my_dict.get('input_csv2')
        self.output_location = my_dict.get('output_location')

    def reading_cleaned_csv(self, input_csv):
        """
        Description:
            This function is used to perform read the csv file and return the dataframe.
        Parameter:
            input_csv: input_csv file to be checked.
        Return:
            Returns tweet_df
        """
        try:
            input_schema = StructType([
                StructField('index', StringType(), nullable=True),
                StructField('tweet_id', StringType(), nullable=True),
                StructField('name', StringType(), nullable=True),
                StructField('tweets', StringType(), nullable=True),
                StructField('followers', IntegerType(), nullable=True),
                StructField('friends', IntegerType(), nullable=True),
                StructField('verified', StringType(), nullable=True),
                StructField('total_post', IntegerType(), nullable=True),
                StructField('profile_image', StringType(), nullable=True),
                StructField('location', StringType(), nullable=True)
            ])
            tweet_df = self.spark.read.format('csv') \
                .option('header', True) \
                .schema(input_schema) \
                .option('mode', 'DROPMALFORMED') \
                .option('path', input_csv) \
                .load()
            tweet_df.printSchema()
            return tweet_df
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")
            self.spark.stop()

    def dropping_index(self, cleaned_data):
        """
        Description:
            This function is used to drop the index in a dataframe.
        Parameter:
            cleaned_data: cleaned_data file to be checked.
        Return:
            Returns df
        """
        try:
            # Dropping the index column
            df = cleaned_data.drop('index')
            print(df.describe().show())
            print(f"Twitter dataset: \n{df.show(truncate=False, vertical=False)}")
            return df
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")
            self.spark.stop()

    def union_of_data(self):
        """
        Description:
            This function is used to perform the union of dataframe.
        Parameter:
            None
        Return:
            Returns df_union
        """
        try:
            cleaned_data1 = self.reading_cleaned_csv(self.input_csv1)
            df = self.dropping_index(cleaned_data1)
            cleaned_data2 = self.reading_cleaned_csv(self.input_csv2)
            df1 = self.dropping_index(cleaned_data2)
            df_union = df.union(df1)
            return df_union
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")
            self.spark.stop()

    def pre_processing_the_data(self):
        """
        Description:
            This function is used to perform the pre-processing of data.
        Parameter:
            None
        Return:
            Returns final_df
        """
        try:
            df_union = self.union_of_data()
            print(df_union.describe().show())
            print(f"Union of datsets: \n{df_union.show(truncate=False, vertical=False)}")
            # Null value validation
            final_df = df_union.fillna(value='Not available', subset=['location'])
            print(final_df.describe().show())
            print(f"Null value: \n{final_df.show(truncate=False, vertical=False)}")
            # Filter
            filter_df = final_df.filter(final_df.name == 'Oliver')
            print(filter_df.describe().show())
            print(f"Filter: \n{filter_df.show(truncate=False, vertical=False)}")
            # Group-By
            group_by_df = final_df.select('name', 'followers').groupby('followers')
            print(f"Group by: \n{group_by_df.count().show(truncate=False, vertical=False)}")
            return final_df
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")
            self.spark.stop()

    def writing_output_to_local_and_hdfs(self):
        """
        Description:
            This function is used to writing csv file to local directory and hdfs location.
        Parameter:
            None
        Return:
            Returns None
        """
        try:
            final_df = self.pre_processing_the_data()
            # Writing it to the output location
            final_df.coalesce(1).write.mode('overwrite').option('header', True).csv(self.output_location)
            # Storing Result in HDFS
            path = self.output_location
            file_list = os.listdir(path)[2]
            file_path = path + file_list
            # Command to put file into hdfs location
            os.popen(f'hadoop fs -put {file_path} /data/')
            # Command to list the file inside the hdfs location
            os.popen('hadoop fs -ls /data/')
            self.upload_blob_to_azure_blob_storage(self.output_location, self.blob_name)
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")
            self.spark.stop()

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
            folder_name = self.output_location
            file = os.listdir(folder_name)[2]
            with open(folder_name + file, 'rb') as blob:
                blob_client.upload_blob(data=blob, overwrite=True)
            print('File Successfully Uploaded')
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")
            self.spark.stop()

def main():
    try:
        spark = SparkSession.builder \
            .master('local[2]') \
            .appName('Streaming application') \
            .getOrCreate()
        load_dotenv()
        connect_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        container = 'csvs-to-azure-blob'
        blob_name = 'tweepy_output_dest.csv'
        input_csv1 = "D:/DataEngg/CFP_Assignment/Twitter_streaming/tweet_data.csv"
        input_csv2 = "D:/DataEngg/CFP_Assignment/Twitter_streaming/tweet_data1.csv"
        output_location = "D:/DataEngg/CFP_Assignment/Twitter_streaming/output/"
        my_dict = {'spark': spark, 'connect_string': connect_string, 'container': container,
                   'blob_name': blob_name, 'input_csv1': input_csv1, 'input_csv2': input_csv2,
                   'output_location': output_location}
        return my_dict
    except Exception:
        exception_type, _, exception_traceback = sys.exc_info()
        line_number = exception_traceback.tb_lineno
        lg.exception(
            f"Exception type : {exception_type} \nError on line number : {line_number}")
        spark.stop()

if __name__ == '__main__':
    my_dict = main()
    tweet_obj = PreprocessingLoadToBlobStorage(my_dict)
    tweet_obj.union_of_data()
    tweet_obj.writing_output_to_local_and_hdfs()





































    # # Read from the stream
    # stream_df = spark \
    #     .readStream \
    #     .format('csv') \
    #     .schema(input_schema) \
    #     .load(path="InputData")
    # stream_df.printSchema()
    #
    # stream_df_query = stream_df\
    #     .writeStream\
    #     .format('console')\
    #     .start()
    # stream_df_query.awaitTermination()
    #
    # print('Application Completed')




