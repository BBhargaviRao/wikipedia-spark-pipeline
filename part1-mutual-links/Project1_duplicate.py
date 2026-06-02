import pyspark
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    #.remote('sc://localhost:15002')
    .appName("My App")
    .getOrCreate()
)


# In[8]:


#Load data from S3
page_df=spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/page/")
page_df.show()


# In[9]:


def parquet(prefix):
    files = spark.read.parquet(f"s3://bsu-c535-fall2024-commons/arjun-workspace/{prefix}/")
    files.save(prefix)


# In[10]:


import datetime
from datetime import datetime


# In[11]:


def read_parquet(prefix):
    return spark.read.parquet(f"s3://bsu-c535-fall2024-commons/arjun-workspace/{prefix}/")


# In[12]:


pagelinks_df = read_parquet("pagelinks")
redirect_df = read_parquet("redirect")
linktarget_df = spark.read.parquet("s3://bsu-c535-fall2024-commons/arjun-workspace/linktarget/")
pagelinks_df.show()
redirect_df.show()
linktarget_df.show()


# In[13]:


# Create a temporary view for pagelinks and redirects
pagelinks_df.createOrReplaceTempView("pagelinks")
redirect_df.createOrReplaceTempView("redirect")


# In[14]:


redirect_df.printSchema()


# In[15]:


redirect_mapping = (
    spark.sql("""
    SELECT rd_from AS redirect_id, rd_title AS target_id
    FROM redirect
    """)
    .distinct()
)


# In[16]:


result_df = (
    linktarget_df
    .join(
        page_df, 
        (linktarget_df.lt_title == page_df.page_title) & (page_df.page_namespace == linktarget_df.lt_namespace),
        "inner"
    )
)


# In[17]:


result_df.show()


# In[18]:


print(result_df.count())


# In[22]:


# Join pagelinks with result_df to get the linked pages
linked_pages_df = (
    pagelinks_df
    .join(result_df,
          pagelinks_df.pl_target_id == result_df.lt_id,
          "inner")
    .select(
        pagelinks_df.pl_from.alias("source_id"),      # Using pl_from as the source page ID
        result_df.lt_id.alias("target_id")            # Getting the target page ID from result_df
    )
)

# Display the resulting DataFrame
linked_pages_df.show()
print(linked_pages_df.count())


# In[21]:


# Join redirect with page to get the target page ID based on rd_title and page_title
redirect_page_df = (
    redirect_df
    .filter(redirect_df.rd_namespace == 0)  # Select only rows with rd_namespace equal to 0
    .join(
        page_df,
        (redirect_df.rd_title == page_df.page_title) & (redirect_df.rd_namespace == page_df.page_namespace),  # Match title and namespace
        "inner"
    )
    .select(
        redirect_df.rd_from.alias("rd_source"),       # Original page that redirects
        page_df.page_id.alias("rd_target")            # Target page ID after the redirect
    )
)

# Display the resulting DataFrame
redirect_page_df.show()
print(redirect_page_df.count())


# In[27]:


from pyspark.sql import functions as F

# Perform left joins between linked_pages_df and redirect_page_df for both source and target
final_linked_pages_df = (
    linked_pages_df
    .join(redirect_page_df.alias("redirect_source"),
          linked_pages_df.source_id == F.col("redirect_source.rd_source"),
          "left")  # Left join for source
    .join(redirect_page_df.alias("redirect_target"),
          linked_pages_df.target_id == F.col("redirect_target.rd_source"),
          "left")  # Left join for target
)

# Create new columns 'source_final' and 'target_final' using coalesce to select the redirected target if it exists
final_linked_pages_df = final_linked_pages_df.withColumn(
    "source_final",
    F.coalesce(F.col("redirect_source.rd_target"), linked_pages_df.source_id)
).withColumn(
    "target_final",
    F.coalesce(F.col("redirect_target.rd_target"), linked_pages_df.target_id)
)

# Select relevant columns 'source_final' and 'target_final' to display side by side
final_linked_pages_df = final_linked_pages_df.select(
    "source_final", 
    "target_final"
)

# Display the resulting DataFrame and count the number of rows
final_linked_pages_df.show()
print(final_linked_pages_df.count())


# In[30]:


# Perform self-join on final_linked_pages_df to find mutual links
mutual_links_df = (
    final_linked_pages_df.alias("df1")
    .join(
        final_linked_pages_df.alias("df2"),
        (F.col("df1.source_final") == F.col("df2.target_final")) &
        (F.col("df1.target_final") == F.col("df2.source_final")),
        "inner"
    )
    .select(
        F.col("df1.source_final").alias("page_a"),
        F.col("df1.target_final").alias("page_b")
    )
    .distinct()  # Remove duplicate pairs if any
)

# Display the resulting DataFrame with mutual links and count the number of rows
mutual_links_df.show()
print(mutual_links_df.count())


# In[29]:


import os
#spark.table("mutual_links_df").write.mode("OVERWRITE").parquet(os.environ['PAGE_PAIRS_OUTPUT'])
mutual_links_df.write.mode("OVERWRITE").parquet(os.environ['PAGE_PAIRS_OUTPUT'])