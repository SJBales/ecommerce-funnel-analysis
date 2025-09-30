from google.cloud import bigquery
import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

# Use the public dataset's project for running queries
client = bigquery.Client(project='product-analytics-portfolio')

# Query to get all the events by user pseudo id, event name and session -------


def ecommerce_loader(return_dict=False):

    # SQL Statement
    event_sql = """WITH stacked_table AS (SELECT event_date,
        event_timestamp,
        user_pseudo_id,
        event_name
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210130`
    WHERE event_name IN ('page_view', 'add_to_cart', 'begin_checkout', 'purchase')
    UNION ALL
    SELECT event_date,
        event_timestamp,
        user_pseudo_id,
        event_name
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`,
        UNNEST(event_params) AS events
    WHERE event_name IN ('page_view', 'add_to_cart', 'begin_checkout', 'purchase')),

    flagged_events AS (SELECT *,
        CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END AS page_view,
        CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END AS add_to_cart,
        CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END as begin_checkout,
        CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END as purchase
    FROM stacked_table)

    SELECT user_pseudo_id,
        MIN(event_date) AS first_event_date,
        MIN(event_timestamp) AS first_event_timestamp,
        CASE WHEN SUM(page_view) > 0 THEN 1 ELSE 0 END viewed_page,
        CASE WHEN SUM(add_to_cart) > 0 THEN 1 ELSE 0 END added_to_cart,
        CASE WHEN SUM(begin_checkout) > 0 THEN 1 ELSE 0 END began_checkout,
        CASE WHEN SUM(purchase) > 0 THEN 1 ELSE 0 END purchased
    FROM flagged_events
    GROUP BY user_pseudo_id;"""

    # Running the query
    event_query = client.query(event_sql).to_dataframe()

    logger.info("Successfully ran event query")

    # Query for intra-session conversion -------------

    # SQL statement
    session_sql = """-- CTE to stack relevant columns from two date tables
    -- Needs to be compressed to query across all tables with a wildcard for production
    WITH stacked_table AS (SELECT DISTINCT user_pseudo_id,
        event_date,
        event_timestamp,
        events.value.int_value AS session,
        event_name
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210130`,
        UNNEST(event_params) AS events
    WHERE event_name IN ('page_view', 'add_to_cart', 'begin_checkout', 'purchase') AND
        events.key = 'ga_session_id'
    UNION ALL
    SELECT DISTINCT user_pseudo_id,
        event_date,
        event_timestamp,
        events.value.int_value AS session,
        event_name
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210130`,
        UNNEST(event_params) AS events
    WHERE event_name IN ('page_view', 'add_to_cart', 'begin_checkout', 'purchase') AND
        events.key = 'ga_session_id'),

    -- CTE to create flagged events by user and session
    flagged_events AS (SELECT *,
        CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END AS page_view,
        CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END AS add_to_cart,
        CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END as begin_checkout,
        CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END as purchase
    FROM stacked_table)

    -- Getting aggregated values by session and user
    SELECT user_pseudo_id,
        session,
        MIN(event_date) AS first_event_date,
        MIN(event_timestamp) AS first_event_timestamp,
        CASE WHEN SUM(page_view) > 0 THEN 1 ELSE 0 END viewed_page,
        CASE WHEN SUM(add_to_cart) > 0 THEN 1 ELSE 0 END added_to_cart,
        CASE WHEN SUM(begin_checkout) > 0 THEN 1 ELSE 0 END began_checkout,
        CASE WHEN SUM(purchase) > 0 THEN 1 ELSE 0 END purchased
    FROM flagged_events
    GROUP BY user_pseudo_id, session;"""

    # Running the Query
    session_query = client.query(session_sql).to_dataframe()

    logger.info("Successfully ran session query")

    # Query for device information ----------

    # SQL statement
    device_sql = """SELECT DISTINCT user_pseudo_id,
        events.value.int_value AS session,
        device.category,
        device.mobile_brand_name,
        device.operating_system
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`,
        UNNEST(event_params) AS events
    WHERE events.key = 'ga_session_id'"""

    # Running the query
    device_query = client.query(device_sql).to_dataframe()

    logger.info("Successfully ran device query")

    # Query for geo location ------------

    # Geo SQL statement
    geo_sql = """SELECT DISTINCT user_pseudo_id,
        events.value.int_value AS session,
        geo.continent,
        geo.country,
        geo.region,
        geo.city
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`,
        UNNEST(event_params) AS events
    WHERE events.key = 'ga_session_id'"""

    # Running the query
    geo_query = client.query(geo_sql).to_dataframe()

    logger.info("Successfully ran geo query")

    if return_dict:
        return {'event_query': event_query,
                'session_query': session_query,
                'device_query': device_query,
                'geo_query': geo_query}
    else:
        return event_query, session_query, device_query, geo_query


if __name__ == "__main__":
    events, session, device, geo = ecommerce_loader()
