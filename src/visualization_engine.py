import matplotlib.pyplot as plt
import seaborn as sns


class ecommerce_viz:
    def __init__(self, processor):
        self.processor = processor

    def plot_conversion(self, remove_pageview=False):

        if remove_pageview:

            row_mask = self.processor.long_events_df['event'] != 'viewed_page'

            sns.barplot(x='event',
                        y='occurence',
                        data=self.processor.long_events_df[row_mask])
            plt.title('Event Conversion Rates')
            plt.ylabel('Conversion Rate')
            plt.xlabel('Event')
            plt.show()

        else:
            sns.barplot(x='event',
                        y='occurence',
                        data=self.processor.long_events_df)
            plt.title('Event Conversion Rates')
            plt.ylabel('Conversion Rate')
            plt.xlabel('Event')
            plt.show()
