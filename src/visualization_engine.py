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
                                 hue='event',
                                 palette='Greens')

            grid.map_dataframe(sns.barplot,
                               x='event',
                               y='occurence')
            grid.set_axis_labels('Event', 'Conversion Rate')
            grid.set_xticklabels(labels=None)
            grid.add_legend(title='Segment')
            plt.title('Customer Segment Conversion Rates')
            plt.show()

        else:
            # Creating plots
            sns.set_theme()
            sns.barplot(x='event',
                        y='occurence',
                        data=self.processor.long_event_df[row_mask],
                        hue='event',
                        palette='Greens')
            plt.title('Event Conversion Rates')
            plt.ylabel('Conversion Rate')
            plt.xlabel('Event')
            plt.show()

    def create_conversion_table(self):
        '''Creates a segment conversion table'''

        if self.processor._created_segments is False:
            ValueError('Kmeans Segments Not Created')

        return self.processor.long_event_df.loc[:, ['kmeans_cluster',
                                                    'event',
                                                    'occurence']]\
            .pivot_table(index='kmeans_cluster',
                         columns='event',
                         values='occurence',
                         aggfunc='mean')\
            .round(3)

    def create_segment_conversion_heatmap(self,
                                          remove_pageview=False) -> None:
        '''Creates a heatmap for segment conversion rates'''

        # Calling the helper method for removing the pageview event
        if remove_pageview:
            row_mask = self.processor\
                .heatmap_conversion_df.index.values != 'Viewed Page'
        else:
            row_mask = np.array([True] * self.processor
                                .heatmap_conversion_df.shape[0])

        # Creating the heatmap
        sns.set_theme()
        sns.heatmap(self.processor.heatmap_conversion_df[row_mask],
                    annot=True,
                    cmap='coolwarm')
        plt.title("Customer Segment Conversion Heatmap")
        plt.show()

    # Method for plotting conversion over time
    def plot_conversion_rates_over_time(self, remove_pageview=False) -> None:

        # Calling the helper method for removing the pageview event
        row_mask = self.remove_pageview_(self.processor.long_event_df,
                                         remove_pageview)

        # Adding the code for creating the plots
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
                    data=self.processor.long_session_df[row_mask],
                    palette='Greens')
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
        plt.title('Relative Importance of K-Means Features for Segments')
        plt.xlabel('Customer Segment')
        plt.show()
