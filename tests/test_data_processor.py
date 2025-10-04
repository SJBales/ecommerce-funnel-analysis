import unittest
import pandas as pd
from src.data_processor import ecommerce_processor


class TestEcommerceProcessor(unittest.TestCase):

    def setUp(self):
        """Runs before each test method"""
        self.obj = ecommerce_processor()

    def test_run_queries(self):
        """Test that run_queries creates a DataFrame"""
        self.obj.run_queries()

        # assertion
        self.assertIsInstance(self.obj.event_df, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
