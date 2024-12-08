from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark import StorageLevel
import os
from pyspark.sql import SparkSession, DataFrame


class DFIO:
    def __init__(self, spark):
        self.spark = spark

    def read(self, path: str) -> DataFrame:
        full_path = f"{os.environ['CS535_S3_WORKSPACE']}{path}"
        return self.spark.read.parquet(full_path)

    def write(self, df: DataFrame, path: str, mode: str = "OVERWRITE") -> None:
        workspace = os.environ['CS535_S3_WORKSPACE']
        df.write.mode(mode).parquet(f"{workspace}{path}")

def parquet(spark,prefix):
    files = spark.read.parquet(f"s3://bsu-c535-fall2024-commons/arjun-workspace/{prefix}/")
    files.persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView(prefix)

def process_wiki_data(page_df, pagelinks_df, redirect_df, linktarget_df):
    if page_df.isEmpty():
        print("page_df is empty")
        return None
    if pagelinks_df.isEmpty():
        print("pagelinks_df is empty")
        return None
    if redirect_df.isEmpty():
        print("redirect_df is empty")
        return None
    if linktarget_df.isEmpty():
        print("linktarget_df is empty")
        return None
   

    filtered_page_df = page_df.filter(F.col("page_namespace") == 0)

    # Join linktarget_df with page_df to map link targets to page IDs
    result_df = (
        linktarget_df
        .join(
            filtered_page_df,
            (linktarget_df.lt_title == filtered_page_df.page_title) & (filtered_page_df.page_namespace == linktarget_df.lt_namespace),
            "inner"
        )
    )

    # Join pagelinks_df with result_df to get the linked pages
    linked_pages_df = (
        pagelinks_df
        .join(result_df,
              pagelinks_df.pl_target_id == result_df.lt_id,
              "inner")
        .select(
            pagelinks_df.pl_from.alias("source_id"),  # Using pl_from as the source page ID
            result_df.page_id.alias("target_id")      # Getting the target page ID from result_df
        )
    )

    # Join redirect_df with page_df to resolve redirects
    redirect_page_df = (
        redirect_df
        .filter(redirect_df.rd_namespace == 0)  # Select only rows with rd_namespace equal to 0
        .join(
            filtered_page_df,
            (redirect_df.rd_title == page_df.page_title) & (redirect_df.rd_namespace == filtered_page_df.page_namespace),
            "inner"
        )
        .select(
            redirect_df.rd_from.alias("rd_source"),  # Original page that redirects
            filtered_page_df.page_id.alias("rd_target")       # Target page ID after the redirect
        )
    )

    # Perform left joins on linked_pages_df with redirect_page_df for source and target
    final_linked_pages_df = (
        linked_pages_df
        .join(redirect_page_df.alias("redirect_source"),
              linked_pages_df.source_id == F.col("redirect_source.rd_source"),
              "left")  # Left join for source
        .join(redirect_page_df.alias("redirect_target"),
              linked_pages_df.target_id == F.col("redirect_target.rd_source"),
              "left")  # Left join for target
    )

    # Create 'source_final' and 'target_final' columns using coalesce
    final_linked_pages_df = final_linked_pages_df.withColumn(
        "source_final",
        F.coalesce(F.col("redirect_source.rd_target"), linked_pages_df.source_id)
    ).withColumn(
        "target_final",
        F.coalesce(F.col("redirect_target.rd_target"), linked_pages_df.target_id)
    )

    # Extract mutual links
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
        .filter(F.col("page_a") < F.col("page_b"))
        .distinct()
    )
    mutual_links_df.count()
    mutual_links_df.show()
    return mutual_links_df
    


def find_connected_components(
    mutual_links_df: DataFrame, 
    dfio,
    s3_bucket: str, 
    s3_path: str,
    checkpoint_interval: int = 3,
):
    iteration = 0
    converged = False

    # Create initial DataFrame with `pageid` and `component_id`
    combined_links_df = (
        mutual_links_df.select(F.col("page_a").alias("pageid"))
        .union(mutual_links_df.select(F.col("page_b").alias("pageid")))
        .distinct()
    )
    combined_links_with_id_df = combined_links_df.withColumn("component_id", F.col("pageid"))
    combined_links_with_id_df.persist(StorageLevel.MEMORY_AND_DISK)

    while not converged:
        iteration += 1

        # Create bidirectional edges
        edges_df_1 = mutual_links_df.select(
            F.col("page_a").alias("source_vertex"),
            F.col("page_b").alias("target_vertex")
        )
        edges_df_2 = mutual_links_df.select(
            F.col("page_b").alias("source_vertex"),
            F.col("page_a").alias("target_vertex")
        )
        bidirectional_edges_df = edges_df_1.union(edges_df_2).distinct()
        bidirectional_edges_df.persist(StorageLevel.MEMORY_AND_DISK)

        # Join to propagate component IDs
        result1_df = bidirectional_edges_df.join(
            combined_links_with_id_df,
            bidirectional_edges_df["source_vertex"] == combined_links_with_id_df["pageid"]
        ).select(
            F.col("target_vertex"),  # Destination vertex
            F.col("component_id")    # Respective component ID
        )

        # Union and update component IDs
        union_df = result1_df.union(combined_links_with_id_df.select(
            F.col("pageid").alias("target_vertex"),
            F.col("component_id")
        ))
        updated_components_df = union_df.groupBy("target_vertex").agg(
            F.min("component_id").alias("min_component_id")
        )
        union_df.persist(StorageLevel.MEMORY_AND_DISK)

        # Check for convergence
        changes = (
            combined_links_with_id_df
            .join(updated_components_df, combined_links_with_id_df["pageid"] == updated_components_df["target_vertex"])
            .filter(F.col("component_id") != F.col("min_component_id"))
            .count()
        )
        print(f"Iteration {iteration}: Number of changes = {changes}")
        converged = (changes == 0)

        # Update component IDs
        combined_links_with_id_df = updated_components_df.withColumnRenamed("target_vertex", "pageid").withColumnRenamed(
            "min_component_id", "component_id"
        )

        # # Checkpoint and save to S3 every few iterations
        # if iteration % checkpoint_interval == 0 or converged:
        #     checkpoint_path = f"s3://{s3_bucket}/{s3_path}/iteration_{iteration}/"
        #     combined_links_with_id_df.write.mode("overwrite").parquet(checkpoint_path)
        #     print(f"Checkpoint saved to {checkpoint_path}")
        #     combined_links_with_id_df = spark.read.parquet(checkpoint_path)
        #     print(f"Checkpoint read from {checkpoint_path}")

        if iteration % checkpoint_interval == 0 or converged:
            checkpoint_path = f"s3://{s3_bucket}/{s3_path}/iteration_{iteration}/"
    
        # Write the checkpoint
            dfio.write(combined_links_with_id_df, path=checkpoint_path, mode="overwrite")
            print(f"Checkpoint saved to {checkpoint_path}")
    
        # Read the checkpoint back
            combined_links_with_id_df = dfio.read(path=checkpoint_path)
            print(f"Checkpoint read from {checkpoint_path}")

    return combined_links_with_id_df




# page_df = read_parquet("page")
# pagelinks_df = read_parquet("pagelinks")
# redirect_df = read_parquet("redirect")
# linktarget_df = read_parquet("linktarget")

# # Step 1: Process Wiki data and extract mutual links
# mutual_links_df = process_wiki_data(page_df, pagelinks_df, redirect_df, linktarget_df)

# # Step 2: Find connected components
# s3_bucket = "bhargaviraobucket"
# s3_path = "s3://bhargaviraobucket/iteration_/"
# final_components_df = find_connected_components(mutual_links_df, s3_bucket, s3_path)

# # Show the final connected components
# print("Final connected components:")
# final_components_df.show()
