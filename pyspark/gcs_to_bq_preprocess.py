from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_replace,
    trim,
    split,
    explode,
    posexplode,
    size,
    monotonically_increasing_id,
    col,
    lower,
    length,
    floor,
    collect_list,
    concat_ws,
    struct,
    sort_array,
    transform,
    current_timestamp,
    lit
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder \
    .appName("SDG-RAG-Pipeline") \
    .getOrCreate()

# --------------------------------------------------
# STEP 1 — READ RAW FILES
# --------------------------------------------------

df = spark.read.text(
    "gs://sustainable_practices_639/sustainable_development_goals_report_2025.txt"
)

# Original column:
# value

# --------------------------------------------------
# STEP 2 — CLEAN TEXT
# --------------------------------------------------

df = df.withColumn(
    "clean_text",
    lower(col("value"))
)

df = df.withColumn(
    "clean_text",
    regexp_replace(
        col("clean_text"),
        r"\n",
        " "
    )
)

df = df.withColumn(
    "clean_text",
    regexp_replace(
        col("clean_text"),
        r"\s+",
        " "
    )
)

df = df.withColumn(
    "clean_text",
    trim(col("clean_text"))
)

# --------------------------------------------------
# STEP 3 — COMBINE ALL LINES INTO ONE DOCUMENT
# --------------------------------------------------

combined_df = df.agg(
    concat_ws(
        " ",
        collect_list("clean_text")
    ).alias("document_text")
)

# --------------------------------------------------
# STEP 4 — SPLIT INTO WORD ARRAY
# --------------------------------------------------

combined_df = combined_df.withColumn(
    "words",
    split(col("document_text"), " ")
)

# --------------------------------------------------
# STEP 5 — CREATE GLOBAL WORD POSITIONS
# --------------------------------------------------

df = combined_df.select(
    posexplode(col("words")).alias(
        "position",
        "word"
    )
)

print("WORD POSITIONS")
df.show(20, truncate=False)

# --------------------------------------------------
# STEP 6 — CREATE CHUNK GROUPS
# Every 50 words = 1 chunk
# --------------------------------------------------

df = df.withColumn(
    "chunk_group",
    floor(col("position") / 50)
)

print("CHUNK GROUPS")
df.select(
    "position",
    "word",
    "chunk_group"
).show(100, truncate=False)

# --------------------------------------------------
# STEP 7 — REBUILD CHUNKS IN CORRECT ORDER
# --------------------------------------------------

chunked_df = df.groupBy(
    "chunk_group"
).agg(
    sort_array(
        collect_list(
            struct("position", "word")
        )
    ).alias("ordered_words")
)

chunked_df = chunked_df.withColumn(
    "chunk_text",
    concat_ws(
        " ",
        transform(
            col("ordered_words"),
            lambda x: x["word"]
        )
    )
)

# --------------------------------------------------
# STEP 8 — ADD METADATA
# --------------------------------------------------

chunked_df = chunked_df.withColumn(
    "chunk_id",
    monotonically_increasing_id()
)

chunked_df = chunked_df.withColumn(
    "char_count",
    length(col("chunk_text"))
)

chunked_df = chunked_df.withColumn(
    "source_file",
    lit("sustainable_development_goals_report_2025.txt")
)

chunked_df = chunked_df.withColumn(
    "ingest_time",
    current_timestamp()
)

# --------------------------------------------------
# STEP 9 — WINDOW FUNCTION
# --------------------------------------------------

window_spec = Window.partitionBy(
    "source_file"
).orderBy(
    "chunk_group"
)

chunked_df = chunked_df.withColumn(
    "chunk_sequence",
    row_number().over(window_spec)
)

# --------------------------------------------------
# STEP 10 — CREATE METADATA TABLE
# --------------------------------------------------

metadata = [
    (
        "sustainable_development_goals_report_2025.txt",
        "Global",
        "Sustainable Development"
    )
]

metadata_df = spark.createDataFrame(
    metadata,
    [
        "source_file",
        "country",
        "topic"
    ]
)

# metadata_df = spark.sparkContext \
#     .parallelize(metadata) \
#     .toDF([
#         "source_file",
#         "country",
#         "topic"
#     ])

# --------------------------------------------------
# STEP 11 — JOIN
# --------------------------------------------------

final_df = chunked_df.join(
    metadata_df,
    "source_file",
    "left"
)

# --------------------------------------------------
# STEP 12 — FILTER BAD CHUNKS
# --------------------------------------------------

final_df = final_df.filter(
    col("char_count") > 100
)

print("FINAL DATA")

final_df.select(
    "chunk_group",
    "chunk_sequence",
    "country",
    "topic",
    "char_count",
    "chunk_text"
).show(20, truncate=False)

# Drop helper nested column before BigQuery write
final_df = final_df.drop("ordered_words")

# --------------------------------------------------
# STEP 13 — WRITE TO BIGQUERY
# --------------------------------------------------

final_df.write.format("bigquery") \
    .option(
        "table",
        "project-2a49d3eb-fbd7-4884-b79.rag_dataset.sdg_chunks"
    ).option("temporaryGcsBucket", "sustainable_practices_639") \
    .mode("overwrite") \
    .save()

print("Pipeline completed successfully")

# gcloud dataproc jobs submit pyspark gcs_to_bq.py \
#     --cluster=rag-cluster \
#     --region=us-central1