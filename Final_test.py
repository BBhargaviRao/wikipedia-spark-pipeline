import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from Final_P2 import process_wiki_data, find_connected_components
import unittest

class DFIO:
    def __init__(self, spark: SparkSession):
        """
        Initializes DFIO with the given Spark session and base directory for data files.
        
        :param spark: The Spark session to use for reading/writing DataFrames
        """
        self.spark = spark
        # Use environment variable to get the base directory for your files, fallback to a default
        self.base_dir = os.getenv("DATA_BASE_DIR", "data")  # Relative or environment-based path

    def read(self, path: str):
        """
        Reads a JSON file into a DataFrame from the base directory.

        :param file_name: The name of the file to read (e.g., "page.jsonl")
        :return: A DataFrame containing the data from the JSON file
        """
        # Construct the full path to the file
        pathname = os.path.join(self.base_dir, path)
        return self.spark.read.json(pathname)

    def write(self, df: DataFrame, path) -> None:
        """
        Writes a DataFrame to a JSON file in the base directory.

        :param df: The DataFrame to write
        :param file_name: The name of the file to write (e.g., "output.json")
        :param mode: The write mode, defaults to "OVERWRITE"
        """
        # Construct the full path to the file
        pathname = os.path.join(self.base_dir, path)
        os.makedirs(os.path.dirname(pathname),exist_ok=True)

        #df.write.mode(mode).json(path)
        df.coalesce(1).write.mode("overwrite").json(pathname)


def read_json(spark, file_name: str, schema: StructType) -> DataFrame:
    # Use environment variable to get the base directory for your files, fallback to a default
    base_dir = os.getenv("DATA_BASE_DIR", "data")  # Relative or environment-based path

    # Construct the full path by joining the base directory and the file name
    path = os.path.join(base_dir, file_name)

    # Debugging line to verify the path
    print(f"Reading JSON from: {path}")

    # Check if the file exists before reading it
    if not os.path.exists(path):
        raise FileNotFoundError(f"File '{file_name}' not found in {base_dir}. Please make sure the file exists.")

    # Read the JSON file using the provided schema
    return spark.read.schema(schema).json(path)


exp_links = frozenset({
    frozenset([4, 5]),
    frozenset([9, 10])
})
exp_components = frozenset({
    frozenset([9]),
    frozenset([4])
})


class TestMutualLinks(unittest.TestCase):
    def test_create_links_and_components(self):
        spark = SparkSession.builder \
            .appName("Test") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .getOrCreate()

        dfio = DFIO(spark)

        # Schema for "page" table
        page_schema = StructType([
            StructField("page_id", IntegerType(), True),
            StructField("page_title", StringType(), True),
            StructField("page_namespace", IntegerType(), True),
            StructField("page_content_model", StringType(), True),
            StructField("page_is_redirect", StringType(), True),
        ])

        # Schema for "pagelinks" table
        pagelinks_schema = StructType([
            StructField("pl_from", IntegerType(), True),
            StructField("pl_from_namespace", IntegerType(), True),
            StructField("pl_target_id", IntegerType(), True),
        ])

        # Schema for "redirect" table
        redirect_schema = StructType([
            StructField("rd_from", IntegerType(), True),
            StructField("rd_namespace", IntegerType(), True),
            StructField("rd_title", StringType(), True),
            StructField("rd_fragment", StringType(), True),
        ])

        # Schema for "linktarget" table
        linktarget_schema = StructType([
            StructField("lt_id", IntegerType(), True),
            StructField("lt_title", StringType(), True),
            StructField("lt_namespace", IntegerType(), True),
        ])

        # Read data using the updated method
        page_df = read_json(spark, "page.jsonl", page_schema)
        pagelinks_df = read_json(spark, "pagelinks.jsonl", pagelinks_schema)
        redirect_df = read_json(spark, "redirect.jsonl", redirect_schema)
        linktarget_df = read_json(spark, "linktarget.jsonl", linktarget_schema)

        # Show the data for verification
        page_df.show()
        pagelinks_df.show()
        redirect_df.show()
        linktarget_df.show()

        # Process the wiki data to find mutual links
        mutual_links = process_wiki_data(page_df, pagelinks_df, redirect_df, linktarget_df)
        mutual_links = mutual_links.filter(F.col("page_a").isNotNull() & F.col("page_b").isNotNull())

        # Collect the links as frozensets
        actual_links = frozenset(frozenset([row.page_a, row.page_b]) for row in mutual_links.collect())
        self.assertEqual(actual_links, exp_links)

        
        connected_components = find_connected_components(mutual_links, dfio)
        
        connected_components.show()

        # Write the connected components to a file
        dfio.write(connected_components, "wikipedia_components")

        # Group the connected components and compare with expected components
        groups = frozenset(
        frozenset(c.members) for c in connected_components.groupby("pageid")
        .agg(F.collect_list(F.col('component_id')).alias("members")).collect()
        )

        self.assertEqual(groups, exp_components)


if __name__ == "__main__":
   unittest.main()
