import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


class ecommerceViz:
    def __init__(self, processor):
        self.processor = processor

    '''Section for helper functions'''
    # Helper function for removing the pageview event
    def remove_pageview_(self, df, remove_pageview=False) -> np.array:

        if remove_pageview:
            mask = df['event'] != 'Viewed Page'
        else:
            mask = np.array([True] * df.shape[0])

        return mask

    '''Section for overall event conversion'''
    # Method to plot conversion rates
    def plot_conversion_rate(self,
                             remove_pageview=False,
                             plot_segments=False,
                             col_wrap=5) -> None:

        # Calling the helper method for removing the pageview event
        row_mask = self.remove_pageview_(self.processor.long_event_df,
                                         remove_pageview)

        # Adding logic for dynamic plotting of customer segments
        if plot_segments:
            if not self.processor.created_segments:
                raise ValueError("No segments in long dataframe")

            grid = sns.FacetGrid(data=self.processor.long_event_df[row_mask],
                                 col='kmeans_cluster',
                                 col_wrap=col_wrap,
                                 palette='tab10')

            grid.map_dataframe(sns.barplot,
                               x='event',
                               y='occurence',
                               hue='event')
            grid.set_axis_labels('Event', 'Conversion Rate')
            grid.set_xticklabels(labels=None)
            grid.add_legend()
            plt.show()

        else:
            # Creating plots
            sns.set_theme()
            sns.barplot(x='event',
                        y='occurence',
                        data=self.processor.long_event_df[row_mask])
            plt.title('Event Conversion Rates')
            plt.ylabel('Conversion Rate')
            plt.xlabel('Event')
            plt.show()

    # Method for plotting conversion over time
    def plot_conversion_rates_over_time(self, remove_pageview=False) -> None:

        # Calling the helper method for removing the pageview event
        row_mask = self.remove_pageview_(self.processor.long_event_df,
                                         remove_pageview)

        # Using the helper method
        sns.set_theme()
        sns.lineplot(data=self.processor.long_event_df[row_mask],
                     x='first_event_date',
                     y='occurence',
                     hue='event')
        plt.title('Aggregate Event Conversion Over Time')
        plt.ylabel('Conversion Rate')
        plt.xlabel('Event Date')
        plt.show()

    # Method to plot aggregate conversion rates over time
    def plot_conversion_events_over_time(self, remove_pageview=False) -> None:

        # Calling the helper method for removing the pageview event
        row_mask = self.remove_pageview_(self.processor.agg_long_event_df,
                                         remove_pageview)

        sns.set_theme()
        sns.lineplot(data=self.processor.agg_long_event_df[row_mask],
                     x='first_event_date',
                     y='occurence',
                     hue='event')
        plt.title('Aggregate Event Conversion Over Time')
        plt.ylabel('Conversion Events')
        plt.xlabel('Event Date')
        plt.show()

    '''Section for session conversion plots'''
    # Method to plot session conversion
    def plot_session_conversion_rate(self, remove_pageview=False):

        # Calling the helper method for removing the pageview event
        row_mask = self.remove_pageview_(self.processor.long_session_df,
                                         remove_pageview)

        # Creating plots
        sns.set_them()
        sns.barplot(x='event',
                    y='occurence',
                    data=self.processor.long_session_df[row_mask])
        plt.title('Session Conversion Rates')
        plt.ylabel('Conversion Rate')
        plt.xlabel('Event')
        plt.show()

    '''Section for segments plots'''
    def plot_segment_heatmap(self, heatmap_df):
        '''Method for plotting the segments heatmap to describe segments'''

        sns.set_theme()
        sns.heatmap(heatmap_df,
                    cmap='coolwarm')
        plt.title('Segments Heatmap')
        plt.show()
