import logging
import pandas as pd
from src.data_processor import ecommerceProcessor
from functools import reduce
from sklearn.cluster import KMeans

# Setting up a logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class customerSegmentation:
    '''Class for segmenting ecommerce customers'''

    def __init__(self, processor):
        self.processor = processor
        self.customer_df = None
        self.cluster_df = None
        self._kmeans_created_ = False

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
                                  left.merge(right,
                                             on='user_pseudo_id',
                                             how='left'),
                                  dfs_to_join)

        logger.info("Successfully prepped customer dataframe")

    # Getting time-based cohorts
    def get_time_cohorts(self):
        '''Preps data and does conversions needed for time cohorts'''

        self.customer_df['first_event_date'] = pd.to_datetime(
            self.customer_df['first_event_date'])

        min_date = self.customer_df['first_event_date'].min()

        self.customer_df['week'] = self.customer_df['first_event_date']\
            .apply(lambda x: ((x - min_date).days // 7) + 1)

    # Prepating data for clustering
    def prep_clustering_data(self, cols=['user_pseudo_id',
                                         'first_event_date',
                                         'country',
                                         'region']):
        '''Method for preparing data for clustering to id segments'''

        if self.customer_df is None:
            self.prep_segment_data()
            logger.info('No customer DF, prepping for clustering')

        clust_backbone = self.customer_df.loc[:, cols]

        # Only selecting countries that appear a fixed number of times
        country_counts = clust_backbone['country'].value_counts()
        clust_backbone['country_map'] = clust_backbone['country']\
            .apply(lambda x: x if country_counts[x] > 50 else 'other')

        # Only selecting regions that appear a fixed number of times
        region_counts = clust_backbone['region'].value_counts()
        clust_backbone['region_map'] = clust_backbone['region']\
            .apply(lambda x: x if region_counts[x] > 50 else 'other')

        # Selecting final columns and creating dummies
        selected_cluster = clust_backbone.loc[:, ['user_pseudo_id',
                                                  'first_event_date',
                                                  'country_map',
                                                  'region_map']]\
            .set_index('user_pseudo_id')

        self.cluster_df = pd.get_dummies(selected_cluster,
                                         dtype=int,
                                         columns=['first_event_date',
                                                  'country_map',
                                                  'region_map'])
        logger.info('Successfully prepped segmentation data')

    # K-Means clustering
    def create_kmeans(self, centers=10):
        '''Method to identify customer segments using kmeans clustering'''

        k_means = KMeans(n_clusters=centers)
        self.customer_df['kmeans_cluster'] = k_means\
            .fit_predict(self.cluster_df)

        self._kmeans_created_ = True

    # Method for adding clusters to the long events dataframe
    def add_customer_segments(self):
        '''Method that adds customer segments to specified dataframes'''

        # Checking the k-means workflow has been completed
        if not self._kmeans_created_:
            raise Exception('K-Means workflow must be completed first')

        # Dropping the existing k-means column if it's in the dataframe
        if 'kmeans_cluster' in self.processor.long_event_df.columns:
            self.processor.long_event_df.drop(columns='kmeans_cluster')
            logger.info('Dropping exiting kmeans column')

        # Adding the k-means column to long events
        self.processor.long_event_df = pd.merge(
            self.processor.long_event_df.copy(),
            self.customer_df.loc[:, ['user_pseudo_id',
                                     'kmeans_cluster']],
            on='user_pseudo_id',
            how='left')

        self.processor.created_segments = True
        logger.info("Added segments to long events table")


if __name__ == "__main__":
    segments = customerSegmentation(ecommerceProcessor())
    segments.prep_segment_data()
    segments.prep_clustering_data()
