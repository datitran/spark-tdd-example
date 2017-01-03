import os
import sys
import logging
import unittest

try:
    # Append PySpark to PYTHONPATH / Spark 2.1.0
    sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python"))
    sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python", "lib",
                                 "py4j-0.10.4-src.zip"))
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

# change level for logger to suppress gibberish information
logger = logging.getLogger("py4j")
logger.setLevel(logging.WARN)


class BaseTestClass(unittest.TestCase):
    def setUp(self):
        """Create a single node Spark application."""
        conf = SparkConf()
        conf.set("spark.executor.memory", "1g")
        conf.set("spark.cores.max", "1")
        conf.set("spark.app.name", "nosetest")
        SparkSession._instantiatedContext = None
        self.spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
        self.spark_context = self.spark_session.sparkContext

    def tearDown(self):
        """Stop the SparkContext."""
        self.spark_context.stop()
        self.spark_session.stop()
