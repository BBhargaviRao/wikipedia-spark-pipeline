import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, BooleanType
from Final_P2 import process_wiki_data, find_connected_components


class DFIO:
    def __init__(self, spark):
        self.spark = spark

    def read(self, path):
        path = "/Users/bhargaviraobondada/Downloads/test/data"
        return self.spark.read.json(path)

    def write(self, df, path):
        path = f"/Users/bhargaviraobondada/Downloads/test/{path}"
        df.coalesce(1).write.mode("OVERWRITE").json(path)

@staticmethod
# def read_json(spark, path,schema):
#         path = f"./data/{path}"
#         return spark.read.json(path)

def read_json(spark, path, schema):
    print(f"Reading JSON from: {path}")  # Debugging line
    return spark.read.schema(schema).json(path)


exp_links = frozenset({
    (1, 2),  # Mutual links between Page 1 and Page 2
    (2, 3),  # Mutual links between Page 2 and Page 3
    (4, 5),  # Mutual links between Page 4 and Page 5
    (6, 7)   # Mutual links between Page 6 and Page 7
})

exp_components = frozenset([
    frozenset([1, 2, 3]),  # Connected component with pages 1, 2, and 3
    frozenset([4, 5]),     # Connected component with pages 4 and 5
    frozenset([6, 7])      # Connected component with pages 6 and 7
])


class TestMutualLinks(unittest.TestCase):
    def test_create_links_and_components(self):
        spark = SparkSession.builder \
            .appName("Test") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .getOrCreate()

        dfio = DFIO(spark)

    
            # Schema for "page" table
        page_schema = StructType([
                StructField("page_id", LongType(), True),
                StructField("page_title", StringType(), True),
                StructField("page_namespace", IntegerType(), True),
                StructField("page_content_model", StringType(), True),
                StructField("page_is_redirect", BooleanType(), True),
            ])

            # Schema for "pagelinks" table
        pagelinks_schema = StructType([
                StructField("pl_from", LongType(), True),
                StructField("pl_from_namespace", IntegerType(), True),
                StructField("pl_target_id", LongType(), True),
            ])

            # Schema for "redirect" table
        redirect_schema = StructType([
                StructField("rd_from", LongType(), True),
                StructField("rd_namespace", IntegerType(), True),
                StructField("rd_title", StringType(), True),
                StructField("rd_fragment", StringType(), True),
            ])

            # Schema for "linktarget" table
        linktarget_schema = StructType([
                StructField("lt_id", LongType(), True),
                StructField("lt_title", StringType(), True),
                StructField("lt_namespace", IntegerType(), True),
            ])

        #     return page_schema, pagelinks_schema, redirect_schema, linktarget_schema

        # page_schema, pagelinks_schema, redirect_schema, linktarget_schema = get_schemas()

        # # Reading data
        # page_file_path = "./test/data/page.jsonl"
        # linktarget_file_path = "./test/data/linktarget.jsonl"
        # pagelinks_file_path = "./test/data/pagelinks.jsonl"
        # redirect_file_path = "./test/data/redirect.jsonl"

        # # Load JSON files with their respective schemas
        # page_df = spark.read.schema(page_schema).json(page_file_path)
        # pagelinks_df = spark.read.schema(linktarget_schema).json(linktarget_file_path)
        # redirect_df = spark.read.schema(pagelinks_schema).json(pagelinks_file_path)
        # linktarget_df = spark.read.schema(redirect_schema).json(redirect_file_path)
        # print(page_df)

        # Read JSON files with schemas using the function

        page_df = read_json(spark, "/Users/bhargaviraobondada/Downloads/test/data/linktarget.jsonl", page_schema)
        pagelinks_df = read_json(spark, "/Users/bhargaviraobondada/Downloads/test/data/page.jsonl", pagelinks_schema)
        redirect_df = read_json(spark, "/Users/bhargaviraobondada/Downloads/test/data/redirect.jsonl", redirect_schema)
        linktarget_df = read_json(spark, "/Users/bhargaviraobondada/Downloads/test/data/linktarget.jsonl", linktarget_schema)

        # Show the data for verification
        page_df.show()
        pagelinks_df.show()
        redirect_df.show()
        linktarget_df.show()


        # Process the wiki data to find mutual links
        mutual_links = process_wiki_data(
            spark, page_df, linktarget_df, pagelinks_df, redirect_df
        )

        # Write mutual links to a file
        dfio.write(mutual_links, "mutual_links")

        # Collect the actual mutual links and compare with the expected ones
        actual_links = frozenset(tuple(r) for r in mutual_links.collect())
        self.assertEqual(actual_links, exp_links)

        # Prepare the edges for connected components
        edges = mutual_links.selectExpr("page_a as src", "page_b as dst")

        # Find connected components
        cc = find_connected_components(spark, mutual_links, dfio)

        # Write the connected components to a file
        dfio.write(cc, "wikipedia_components")

        # Group the connected components and compare with expected components
        groups = frozenset(
            frozenset(c.members) for c in cc.groupby("component")
            .agg(F.expr("collect_list(vertex) as members")).collect()
        )
        self.assertEqual(groups, exp_components)


if __name__ == "__main__":
    unittest.main()
