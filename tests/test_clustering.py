from . import basetests
import clustering


class ClusteringTest(basetests.BaseTestClass):
    def mock_data(self):
        """Mock data to imitate read from database."""
        mock_data_rdd = self.spark_context.parallelize([("A", 1, 1), ("B", 1, 0),
                                                        ("C", 0, 2), ("D", 2, 4),
                                                        ("E", 3, 5)])
        schema = ["id", "x", "y"]
        mock_data_df = self.spark_session.createDataFrame(mock_data_rdd, schema)
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
                         [0, 0, 0, 1, 1])
