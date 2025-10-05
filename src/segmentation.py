import logging
import pandas as pd
from src.data_processor import ecommerceProcessor
from functools import reduce

# Setting up a logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class customerSegmentation:
    '''Class for segmenting ecommerce customers'''

    def __init__(self, processor):
        self.processor = processor
        self.customer_df = None

    def prep_segment_data(self) -> None:
        '''Creates dataframe for segmentation'''

        # Ensuring the processsor has data
        if self.processor.event_df is None:
            self.processor.run_queries()
            logger.info("No processor initialized, running setup")

        # Prepping event table for join
        event_for_join = self.processor.event_df\
            .copy().loc[:, ['user_pseudo_id',
                            'first_event_date']]

        # Prepping geo table for join
        geo_for_join = self.processor.geo_df\
            .copy().loc[:, ['user_pseudo_id',
                            'continent',
                            'country',
                            'region',
                            'city']].drop_duplicates()

        # Prepping device table for join
        device_for_join = self.processor.device_df\
            .copy().loc[:, ['user_pseudo_id',
                            'category',
                            'mobile_brand_name',
                            'operating_system']].drop_duplicates()

        dfs_to_join = [event_for_join, geo_for_join, device_for_join]

        self.customer_df = reduce(lambda left, right:
                                  left.merge(right, on='user_pseudo_id'),
                                  dfs_to_join)

        logger.info("Successfully prepped customer dataframe")

    def get_time_cohorts(self):
        '''Preps data and does conversions needed for time cohorts'''

        self.customer_df['first_event_date'] = pd.to_datetime(
            self.customer_df['first_event_date'])

        min_date = self.customer_df['first_event_date'].min()

        self.customer_df['week'] = self.customer_df['first_event_date']\
            .apply(lambda x: ((x - min_date).days // 7) + 1)


if __name__ == "__main__":
    segments = customerSegmentation(ecommerceProcessor())
    segments.prep_segment_data()
    print(segments.customer_df.head())
