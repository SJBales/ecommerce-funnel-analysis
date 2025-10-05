import unittest
import pandas as pd
from src.data_processor import ecommerceProcessor


class TestEcommerceProcessor(unittest.TestCase):

    def setUp(self):
        """Runs before each test method"""
        self.obj = ecommerceProcessor()
        self.obj.run_queries()

    def test_run_queries(self):
        """Test that run_queries creates a DataFrame"""

        # Assertions that attributes have been created are pdDF
        self.assertIsInstance(self.obj.event_df, pd.DataFrame)
        self.assertIsInstance(self.obj.session_df, pd.DataFrame)
        self.assertIsInstance(self.obj.device_df, pd.DataFrame)
        self.assertIsInstance(self.obj.geo_df, pd.DataFrame)

    def test_event_prep(self):
        '''Test for the events prep'''
        self.obj.prep_events()

        # Assertion that the reshape method was successful
        self.assertEqual(set(self.obj.long_event_df.columns),
                         {'user_pseudo_id',
                          'first_event_date',
                          'first_event_timestamp',
                          'event',
                          'occurence'})
        self.assertGreater(self.obj.long_event_df.shape[0], 0)

    def test_session_prep(self):
        '''Test for the session prep'''
        self.obj.prep_session()

        # Assertion that the reshape method was successful
        self.assertEqual(set(self.obj.long_session_df.columns),
                         {'user_pseudo_id',
                          'session',
                          'first_event_date',
                          'first_event_timestamp',
                          'event',
                          'occurence'})
        self.assertGreater(self.obj.long_session_df.shape[0], 0)


if __name__ == "__main__":
    unittest.main()
