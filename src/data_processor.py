import pandas as pd
from data_loader import ecommerce_loader


# Setting up the class
class ecommerce_processor:
    def __init__(self) -> None:
        self.event_df = None
        self.session_df = None
        self.device_df = None
        self.geo_df = None

    def run_queries(self) -> None:
        self.event_df, self.session_df, self.device_df, self.geo_df = ecommerce_loader()

    def prep_events(self) -> None:
        self.long_event_df = self.event_results.melt(id_vars=['user_pseudo_id',
                                                              'first_event_date',
                                                              'first_event_timestamp'])\
            .rename(columns={'variable': 'event', 'value': 'occurence'})


if __name__ == "__main__":
    processor = ecommerce_processor()
    processor.run_queries()
