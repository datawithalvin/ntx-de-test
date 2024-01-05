import pandas as pd
import mysql.connector

# Baca file CSV
df = pd.read_csv('data-to-insights.ecommerce.all_sessions-sql.csv', usecols=['channelGrouping', 'country', 'fullVisitorId', 'timeOnSite', 'pageviews', 'sessionQualityDim', 'v2ProductName', 'productRevenue', 'productQuantity', 'productRefundAmount'])

# Koneksikan ke MySQL
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='B2727_pf',
    database='ntx_test'
)

cursor = connection.cursor()

# Masukkan setiap baris dari dataframe ke tabel MySQL
for _, row in df.iterrows():
    cursor.execute("""
    INSERT INTO ecommerce (channelGrouping, country, fullVisitorId, timeOnSite, pageviews, sessionQualityDim, v2ProductName, productRevenue, productQuantity, productRefundAmount)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (row['channelGrouping'], row['country'], row['fullVisitorId'], row['timeOnSite'], row['pageviews'], row['sessionQualityDim'], row['v2ProductName'], row['productRevenue'], row['productQuantity'], row['productRefundAmount']))

# Commit perubahan dan tutup koneksi
connection.commit()
cursor.close()
connection.close()