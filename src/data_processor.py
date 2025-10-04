from data_loader import ecommerce_loader
import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


# Setting up the data processor class
class ecommerce_processor:
    def __init__(self) -> None:
        self.event_df = None
        self.session_df = None
        self.device_df = None
        self.geo_df = None

    # Method for running the queries and storing the results
    def run_queries(self) -> None:
        self.event_df,
        self.session_df,
        self.device_df,
        self.geo_df = ecommerce_loader()

    # Method to pivot the event data from wide to long
    def prep_events(self) -> None:
        self.long_event_df = self.event_df.melt(
            id_vars=['user_pseudo_id',
                     'first_event_date',
                     'first_event_timestamp'])\
            .rename(columns={'variable': 'event', 'value': 'occurence'})
        self._long_event_initialized = True

        logger.INFO("Converted events from wide to long")

    # Method to pivot the session conversion data from wide to long
    def prep_session(self) -> None:
        self.long_session_df = self.session_df.melt(
            id_vars=['user_pseudo_id',
                     'session',
                     'first_event_date',
                     'first_event_timestamp'])\
            .rename(columns={'variable': 'event', 'value': 'occurence'})

        logger.INFO("Converted session from wide to long")

    # Method to get aggregate conversions by event
    def prep_agg_conversion(self) -> None:

        # Checking if events have been prepped
        if not self._long_event_initialized:
            self.prep_events()
        else:
            self.agg_long_events_df = self.long_event_df\
                .loc[:, ['first_event_date',
                         'event',
                         'occurence']]\
                .groupby(['first_event_date', 'event']).sum().reset_index()

        logger.INFO("Aggregated events")


if __name__ == "__main__":
    processor = ecommerce_processor()
    processor.run_queries()
