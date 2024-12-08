from Final_P2 import DFIO, parquet, process_wiki_data, find_connected_components
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("WikiGraphConnectedComponents")
    .getOrCreate()
)

dfio = DFIO(spark)

parquet(spark, 'page')
parquet(spark, 'linktarget')
parquet(spark, 'redirect')
parquet(spark, 'pagelinks') 

page_df = spark.table('page')
linktarget_df = spark.table('linktarget')
redirect_df = spark.table('redirect')
pagelinks_df= spark.table('pagelinks') 


mutual_links = process_wiki_data(page_df, pagelinks_df,redirect_df,linktarget_df)

dfio.write(mutual_links, "mutual_links")
mutual_links = dfio.read("mutual_links")

s3_bucket = "bhargaviraobucket"
s3_path = "iteration_"

connected_components = find_connected_components(mutual_links, dfio, s3_bucket, s3_path)

dfio.write(connected_components, "wikipedia_components")

cc_count = connected_components.groupby('component_id').count()
cc_count.orderBy('count', ascending=False).show()
