from . import basetests
from pyspark.sql.types import *

import os
import clustering  # script file needs to be imported after basetests


class ClusteringTest(basetests.BaseTestClass):
    def mock_data(self):
        """Mock data to imitate read from database."""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("x", DoubleType(), True),
            StructField("y", DoubleType(), True),
        ])

        mock_data_df = (self.spark_session
            .read
            .format("csv")
            .schema(schema)
            .load(os.path.join(os.path.dirname(
            os.path.abspath(__file__)), "mock_data.csv"))
        )
        return mock_data_df

    def test_count(self):
        """Check if mock data has five rows."""
        self.assertEqual(self.mock_data().count(), 5)

    def test_convert_df(self):
        """Check if dataframe has the form (id, DenseVector)."""
        input_df = clustering.convert_df(self.spark_session, self.mock_data())
        self.assertEqual(input_df.dtypes, [("id", "string"),
                                           ("features", "vector")])

    def test_rescale_df_first_entry(self):
        """Check if rescaling works for the first entry of the first row."""
        input_df = clustering.convert_df(self.spark_session, self.mock_data())
        scaled_df = clustering.rescale_df(input_df)
        self.assertAlmostEqual(scaled_df.rdd.map(lambda x: x.features_scaled)
                               .take(1)[0].toArray()[0], 0.8770580193070292)

    def test_rescale_df_second_entry(self):
        """Check if rescaling works for the second entry of the first row."""
        input_df = clustering.convert_df(self.spark_session, self.mock_data())
        scaled_df = clustering.rescale_df(input_df)
        self.assertAlmostEqual(scaled_df.rdd.map(lambda x: x.features_scaled)
                               .take(1)[0].toArray()[1], 0.48224282217041214)

    def test_assign_cluster(self):
        """Check if rows are labeled as expected."""
        input_df = clustering.convert_df(self.spark_session, self.mock_data())
        scaled_df = clustering.rescale_df(input_df)
        label_df = clustering.assign_cluster(scaled_df)
        self.assertEqual(label_df.rdd.map(lambda x: x.label).collect(),
                         [1, 1, 1, 0, 0])
