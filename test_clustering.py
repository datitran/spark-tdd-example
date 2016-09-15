import os
import sys
import unittest

try:
    # Append PySpark to PYTHONPATH / Spark 2.0.0
    sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python"))
    sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python", "lib",
                                 "py4j-0.10.1-src.zip"))
except KeyError as e:
    print("SPARK_HOME is not set", e)
    sys.exit(1)

try:
    # Import PySpark modules here
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
except ImportError as e:
    print("Can not import Spark modules", e)
    sys.exit(1)

# Import script modules here
import clustering

class ClusteringTest(unittest.TestCase):

    def setUp(self):
        """Create a single node Spark application."""
        conf = SparkConf()
        conf.set("spark.executor.memory", "1g")
        conf.set("spark.cores.max", "1")
        conf.set("spark.app.name", "nosetest")
        SparkSession._instantiatedContext = None
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.sc = self.spark.sparkContext
        self.mock_df = self.mock_data()

    def tearDown(self):
        """Stop the SparkContext."""
        self.sc.stop()
        self.spark.stop()

    def mock_data(self):
        """Mock data to imitate read from database."""
        mock_data_rdd = self.sc.parallelize([("A", 1, 1), ("B", 1, 0),
                                             ("C", 0, 2), ("D", 2, 4),
                                             ("E", 3, 5)])
        schema = ["id", "x", "y"]
        mock_data_df = self.spark.createDataFrame(mock_data_rdd, schema)
        return mock_data_df

    def test_count(self):
        """Check if mock data has five rows."""
        self.assertEqual(len(self.mock_df.collect()), 5)

    def test_convert_df(self):
        """Check if dataframe has the form (id, DenseVector)."""
        input_df = clustering.convert_df(self.spark, self.mock_df)
        self.assertEqual(input_df.dtypes, [("id", "string"),
                                           ("features", "vector")])

    def test_rescale_df_first_entry(self):
        """Check if rescaling works for the first entry of the first row."""
        input_df = clustering.convert_df(self.spark, self.mock_df)
        scaled_df = clustering.rescale_df(input_df)
        self.assertAlmostEqual(scaled_df.rdd.map(lambda x: x.features_scaled)
                               .take(1)[0].toArray()[0], 0.8770580193070292)

    def test_rescale_df_second_entry(self):
        """Check if rescaling works for the second entry of the first row."""
        input_df = clustering.convert_df(self.spark, self.mock_df)
        scaled_df = clustering.rescale_df(input_df)
        self.assertAlmostEqual(scaled_df.rdd.map(lambda x: x.features_scaled)
                               .take(1)[0].toArray()[1], 0.48224282217041214)

    def test_assign_cluster(self):
        """Check if rows are labeled as expected."""
        input_df = clustering.convert_df(self.spark, self.mock_df)
        scaled_df = clustering.rescale_df(input_df)
        label_df = clustering.assign_cluster(scaled_df)
        self.assertEqual(label_df.rdd.map(lambda x: x.label).collect(),
                         [0, 0, 0, 1, 1])
