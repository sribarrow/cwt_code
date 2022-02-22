import unittest
from unittest.mock import Mock
from unittest.mock import patch, create_autospec
from transforms.pyspark_transform import *
from pyspark.sql import SparkSession

class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.master("local[*]") \
            .appName('Python test Spark RDD') \
            .getOrCreate()
        # Create an empty RDD
        emp_RDD = self.spark.sparkContext.emptyRDD()

        # Create empty schema
        self.columns = StructType([])

        # Create an empty RDD with empty schema
        self.data = self.spark.createDataFrame(data=emp_RDD,
                                     schema=self.columns)

    def tearDown(self):
        self.spark.stop()

    def test_spark_session(self):
        df=self.data
        mock_function = create_autospec(data_cleanup, return_value=df)
        self.assertEqual(mock_function(df),df)

    def test_write_file(self):
        mock_function = create_autospec(write_file, return_value='data/csv/dates')
        self.assertEqual(mock_function(self.data,"dates"), 'data/csv/dates')

    def test_read_parquet(self):
        mock_function = create_autospec(read_parquet, return_value=16546)
        self.assertEqual(mock_function(), 16546)

    def test_read_csv(self):
        schema=self.columns
        mock_function = create_autospec(read_csv, return_value=16546)
        self.assertEqual(mock_function('test.csv',schema=schema), 16546)

    def test_get_top_5_revenues(self):
        mock_function = create_autospec(get_top_5_revenues, return_value=5)
        self.assertEqual(mock_function(self.data), 5)

    @patch('transforms.pyspark_transform')
    def test_read_csv_failed(self, mock_requests):
        mock_requests.get.side_effect = Exception
        with self.assertRaises(Exception):
            read_csv()
            mock_requests.get.assert_called_once()




    # def test_read_parquet_success(self):
    #    self.assertEqual(read_parquet(), 16546)

    # def test_read_parquet_failed(self):
    #    self.assertEqual(read_parquet(), 0)

if __name__ == '__main__':
    unittest.main()
