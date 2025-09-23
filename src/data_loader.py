from google.cloud import bigquery

# Use the public dataset's project for running queries
client = bigquery.Client(project='product-analytics-portfolio')

query = """
SELECT table_name 
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name
"""
tables = client.query(query).to_dataframe()
print(tables)

query2 = """
SELECT *
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20201101`
LIMIT 10"""

columns = client.query(query2).to_dataframe()
print(columns)
