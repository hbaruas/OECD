#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# ==========================================
# CELL 1: PHASE 1 - HEAVY NLP EXTRACTION (ARRAY PACKED & SAMPLED)
# ==========================================
import os
import gc
import spacy
import pandas as pd
import logging
from typing import Optional, Literal, Dict
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# --- LOGGER SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
BASE_PATH = "/Users/saurabhkumar/Desktop/OECD_PYSPARK_LOCAL/data/parquet_OECD"
PARQUET_SOURCE = os.path.join(BASE_PATH, "part-00000-6f2787d8-9f9c-4b9b-9903-fc9d83e3d0c0-c000.snappy.parquet")
YEARS = [2020, 2021, 2022, 2023, 2024, 2025]
FORCE_RECOMPUTE_NLP = False 

# 1.0 = All 60 Million records. 0.1 = Random 10% sample. 0.01 = Random 1% sample.
SAMPLE_FRACTION = 1.0  

def get_nlp_path(year):
    return os.path.join(BASE_PATH, "processed_data", str(year), "noun_chunks_packed")

# --- RDSA SPARK SESSION BUILDER ---
def create_spark_session(
    app_name: Optional[str] = None,
    size: Optional[Literal["small", "medium", "large", "extra-large"]] = None,
    extra_configs: Optional[Dict[str, str]] = None,
) -> SparkSession:
    try:
        if size:
            size = size.lower()
            valid_sizes = ["small", "medium", "large", "extra-large"]
            if size not in valid_sizes:
                msg = f"Invalid '{size=}'. If specified must be one of {valid_sizes}."
                raise ValueError(msg)

        logger.info(
            (f"Creating a '{size}' Spark session..." if size else "Creating a basic Spark session...")
        )

        if app_name:
            builder = SparkSession.builder.appName(f"{app_name}")
        else:
            builder = SparkSession.builder

        # fmt: off
        if size == "small":
            builder = (
                builder.config("spark.executor.memory", "1g")
                .config("spark.executor.cores", 1)
                .config("spark.dynamicAllocation.maxExecutors", 3)
                .config("spark.sql.shuffle.partitions", 12)
            )
        elif size == "medium":
            builder = (
                builder.config("spark.executor.memory", "6g")
                .config("spark.executor.cores", 3)
                .config("spark.dynamicAllocation.maxExecutors", 3)
                .config("spark.sql.shuffle.partitions", 18)
            )
        elif size == "large":
            builder = (
                builder.config("spark.executor.memory", "10g")
                .config("spark.yarn.executor.memoryOverhead", "1g")
                .config("spark.executor.cores", 5)
                .config("spark.dynamicAllocation.maxExecutors", 5)
                .config("spark.sql.shuffle.partitions", 200)
            )
        elif size == "extra-large":
            builder = (
                builder.config("spark.executor.memory", "20g")
                .config("spark.yarn.executor.memoryOverhead", "2g")
                .config("spark.executor.cores", 5)
                .config("spark.dynamicAllocation.maxExecutors", 12)
                .config("spark.sql.shuffle.partitions", 240)
            )

        # Common configurations for all sizes
        builder = (
            builder.config("spark.dynamicAllocation.enabled", "true")
             .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.ui.showConsoleProgress", "false")
        ).enableHiveSupport()
        # fmt: on

        # Apply extra configurations
        if extra_configs:
            for key, value in extra_configs.items():
                builder = builder.config(key, value)

        logger.info("Spark session created successfully!")
        return builder.getOrCreate()
    except Exception as e:
        logger.error(f"An error occurred while creating the Spark session: {e}")
        raise

# --- INITIALIZE EXTRA-LARGE CLUSTER ---
spark = create_spark_session(
    app_name="OECD_Phase1_NLP_Packed",
    size="extra-large", 
    extra_configs={
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "5000" # Our vital memory safeguard
    }
)
spark.sparkContext.setLogLevel("ERROR")

# --- NLP UDF (Outputs Arrays) ---
noun_schema_packed = StructType([
    StructField("doc_JobID", StringType()),
    StructField("doc_BGTOcc", StringType()), 
    StructField("doc_year", IntegerType()),
    StructField("doc_month", IntegerType()),
    StructField("noun_chunks", ArrayType(StringType())),
    StructField("sim_scores", ArrayType(DoubleType()))
])

def extract_noun_chunks_packed(iterator):
    try: nlp = spacy.load("en_core_web_lg", disable=["lemmatizer", "ner"])
    except: nlp = spacy.load("en_core_web_sm")
    target = nlp("data")

    for pdf in iterator:
        rows = []
        texts = pdf["full_text"].fillna("").astype(str).tolist()

        meta = list(zip(
            pdf["job_id"].astype(str), 
            pdf["soc_2020"].astype(str),
            pdf["doc_year"],
            pdf["doc_month"]
        ))

        for i, doc in enumerate(nlp.pipe(texts, batch_size=50)):
            chunks_list = []
            sims_list = []

            for chunk in doc.noun_chunks:
                if chunk.has_vector:
                    cleaned = "".join(c for c in chunk.text if not c.isdigit()).strip()
                    if cleaned:
                        chunks_list.append(cleaned.lower())
                        sims_list.append(float(chunk.similarity(target)))

            rows.append({
                'doc_JobID': meta[i][0], 
                'doc_BGTOcc': meta[i][1],
                'doc_year': meta[i][2],
                'doc_month': meta[i][3],
                'noun_chunks': chunks_list, 
                'sim_scores': sims_list
            })
        yield pd.DataFrame(rows) if rows else pd.DataFrame(columns=noun_schema_packed.fieldNames())

# --- EXECUTION ---
for year in YEARS:
    out_path = get_nlp_path(year)
    if not FORCE_RECOMPUTE_NLP:
        try:
            if spark.read.parquet(out_path).limit(1).count() > 0:
                print(f"[SKIP] Data already extracted for {year}.")
                continue
        except: pass

    try:
        print(f"\n--- EXTRACTING TEXT FOR {year} ---")
        df_raw = spark.read.parquet(PARQUET_SOURCE) \
            .withColumn("date", F.to_date("date")) \
            .filter(F.year("date") == year) \
            .withColumn("doc_year", F.year("date")) \
            .withColumn("doc_month", F.month("date"))

        # --- SAMPLING LOGIC & LOGGING APPLIED ---
        if SAMPLE_FRACTION < 1.0:
            df_raw = df_raw.sample(False, SAMPLE_FRACTION, seed=42)

        advert_count = df_raw.count()
        print(f"  -> Processing {advert_count} job adverts (Sample Fraction: {SAMPLE_FRACTION})")

        if advert_count == 0: 
            print(f"  -> Skipping {year}: No records found after sampling.")
            continue

        # Adjust partitions to take full advantage of the 240 partition limit of 'extra-large'
        df_raw = df_raw.repartition(max(240, int(advert_count/10000)))

        chunks = df_raw.mapInPandas(extract_noun_chunks_packed, schema=noun_schema_packed)

        chunks.write.mode("overwrite").partitionBy("doc_month").parquet(out_path)
        print(f"  [DONE] Extracted NLP features for {year}.")

    except Exception as e:
        print(f"  [ERROR] Skipping {year} NLP extraction due to error: {e}")


# In[ ]:


# ==========================================
# CELL 2: PHASE 2 - OECD DICTIONARY (VALID-UNIVERSE & MAX-RF)
# ==========================================
import os
import pyspark.sql.functions as F

print("--- Phase 2: Building OECD Dictionary ---")

# --- FILE PATHS & CONFIGURATION ---
BASE_PATH = "/Users/saurabhkumar/Desktop/OECD_PYSPARK_LOCAL/data/parquet_OECD"
CENSUS_CSV = "/Users/saurabhkumar/Desktop/OECD_PYSPARK_LOCAL/data/Census.csv"
SUT_CSV = "/Users/saurabhkumar/Desktop/OECD_PYSPARK_LOCAL/data/SUT_TABLE.csv"
YEARS = [2020, 2021, 2022, 2023, 2024, 2025]

# --- NEW: EXPORT CONFIGURATION ---
# Create a folder in the exact location the notebook is running
LOCAL_EXPORT_DIR = os.path.join(os.getcwd(), "oecd_pipeline_exports")
os.makedirs(LOCAL_EXPORT_DIR, exist_ok=True)

DICT_CSV_PATH = os.path.join(LOCAL_EXPORT_DIR, "oecd_dictionary_raw.csv")
USE_EXISTING_DICTIONARY = False  # Set to True to skip computation and load from CSV

def get_nlp_path(year):
    return os.path.join(BASE_PATH, "processed_data", str(year), "noun_chunks_packed")

# STRICT OECD PARAMETERS:
REL_SHARE_THRESHOLD = 10.0  
SIM_GROUNDING = 0.35       
DATA_THRESHOLD = 3         

SUT_YEAR = 2023
ALPHA_LOW = 1.58
ALPHA_ECONOMY_AVG = 3.62
ALPHA_MAP = {
    "A": 3.62, "B-E": 6.45, "F": 6.64, "G-I": 2.95, "J": 2.97,
    "K": 3.91, "L": 3.62, "M-N": 2.79, "O-Q": 2.07, "R-T": 3.06, "U": 3.62
}

SOC_GROUPS = {
    "data_entry": ["4152"], 
    "database":   ["99999"], # Dummy code
    "analytics":  ["99999"]  # Dummy code
}
ALL_ANCHOR_SOCS = [item for sublist in SOC_GROUPS.values() for item in sublist]

if USE_EXISTING_DICTIONARY and os.path.exists(DICT_CSV_PATH):
    print(f"Loading existing raw dictionary from {DICT_CSV_PATH}...")
    oecd_vocabulary = spark.read.option("header", True).csv(DICT_CSV_PATH) \
        .withColumn("relative_share", F.col("relative_share").cast("double")) \
        .withColumn("avg_sim", F.col("avg_sim").cast("double")) \
        .withColumn("global_count", F.col("global_count").cast("long")).cache()

    print(f"✓ Loaded {oecd_vocabulary.count()} terms from saved CSV.")

else:
    print("Computing OECD Dictionary (Valid-Universe Denominators & Max-RF)...")
    try:
        packed_chunks = None
        for year in YEARS:
            year_path = get_nlp_path(year)
            try:
                df_year = spark.read.parquet(year_path)
                if packed_chunks is None: packed_chunks = df_year
                else: packed_chunks = packed_chunks.unionByName(df_year, allowMissingColumns=True)
            except: pass

        if packed_chunks is None or packed_chunks.rdd.isEmpty():
            raise ValueError("No parquet data could be loaded.")

        total_jobs_economy = packed_chunks.count()
        missing_soc4 = packed_chunks.filter(F.regexp_extract("doc_BGTOcc", r"(\d{4})", 1) == "").count()

        valid_chunks = packed_chunks.select(
            "doc_JobID", "doc_BGTOcc",
            F.explode(F.arrays_zip("noun_chunks", "sim_scores")).alias("zipped")
        ).select(
            "doc_JobID", "doc_BGTOcc",
            F.regexp_extract("doc_BGTOcc", r"(\d{4})", 1).alias("soc4"),
            F.col("zipped.noun_chunks").alias("noun_chunk"),
            F.col("zipped.sim_scores").alias("sim_data")
        ).filter(
            (F.col("sim_data") >= SIM_GROUNDING) & 
            (F.col("soc4") != "")
        ).cache()

        valid_job_universe = valid_chunks.select("doc_JobID").distinct().count()
        valid_anchor_jobs = valid_chunks.filter(F.col("soc4").isin(ALL_ANCHOR_SOCS)).select("doc_JobID").distinct().count()

        print(f"\n--- DATA QUALITY & COVERAGE LOG ---")
        print(f"Jobs dropped due to missing/unparseable SOC: {missing_soc4}")
        print(f"Total Jobs in Economy: {total_jobs_economy}")
        print(f"Total Economy Jobs with >= 1 valid data chunk: {valid_job_universe}")
        print(f"Total Anchor Jobs with >= 1 valid data chunk: {valid_anchor_jobs}")
        print(f"NLP Coverage Rate: {round((valid_job_universe/(total_jobs_economy if total_jobs_economy > 0 else 1))*100, 2)}%\n")

        valid_jobs_entry = valid_chunks.filter(F.col("soc4").isin(SOC_GROUPS["data_entry"])).select("doc_JobID").distinct().count()
        valid_jobs_db = valid_chunks.filter(F.col("soc4").isin(SOC_GROUPS["database"])).select("doc_JobID").distinct().count()
        valid_jobs_ana = valid_chunks.filter(F.col("soc4").isin(SOC_GROUPS["analytics"])).select("doc_JobID").distinct().count()

        global_freq = valid_chunks.groupBy("noun_chunk") \
            .agg(F.countDistinct("doc_JobID").alias("global_count"), F.avg("sim_data").alias("avg_sim")) \
            .withColumn("share_economy", F.col("global_count") / (valid_job_universe if valid_job_universe > 0 else 1))

        entry_freq = valid_chunks.filter(F.col("soc4").isin(SOC_GROUPS["data_entry"])) \
            .groupBy("noun_chunk").agg(F.countDistinct("doc_JobID").alias("count_entry")) \
            .withColumn("share_entry", F.col("count_entry") / (valid_jobs_entry if valid_jobs_entry > 0 else 1))

        db_freq = valid_chunks.filter(F.col("soc4").isin(SOC_GROUPS["database"])) \
            .groupBy("noun_chunk").agg(F.countDistinct("doc_JobID").alias("count_db")) \
            .withColumn("share_db", F.col("count_db") / (valid_jobs_db if valid_jobs_db > 0 else 1))

        ana_freq = valid_chunks.filter(F.col("soc4").isin(SOC_GROUPS["analytics"])) \
            .groupBy("noun_chunk").agg(F.countDistinct("doc_JobID").alias("count_ana")) \
            .withColumn("share_ana", F.col("count_ana") / (valid_jobs_ana if valid_jobs_ana > 0 else 1))

        dictionary_df = global_freq \
            .join(entry_freq, "noun_chunk", "left").fillna(0) \
            .join(db_freq, "noun_chunk", "left").fillna(0) \
            .join(ana_freq, "noun_chunk", "left").fillna(0)

        dictionary_df = dictionary_df \
            .withColumn("rf_entry", F.col("share_entry") / F.col("share_economy")) \
            .withColumn("rf_db", F.col("share_db") / F.col("share_economy")) \
            .withColumn("rf_ana", F.col("share_ana") / F.col("share_economy")) \
            .withColumn("max_relative_share", F.greatest("rf_entry", "rf_db", "rf_ana"))

        oecd_vocabulary = dictionary_df.filter(
            (F.col("max_relative_share") >= REL_SHARE_THRESHOLD) & 
            (F.col("global_count") >= 500) &
            (F.col("avg_sim") >= SIM_GROUNDING)
        ).select("noun_chunk", F.col("max_relative_share").alias("relative_share"), "avg_sim", "global_count").cache()

        # SAVE TO CSV SO WE DON'T HAVE TO DO THIS AGAIN
        pdf_vocab = oecd_vocabulary.toPandas()
        pdf_vocab.to_csv(DICT_CSV_PATH, index=False)

        dict_size = oecd_vocabulary.count()
        print(f"✓ OECD Dictionary Built & Saved. Identified {dict_size} stable data-work terms.")
        print(f"=== TRUE TOP 30 OECD DICTIONARY TERMS (By Volume) ===")
        oecd_vocabulary.orderBy(F.col("global_count").desc()).limit(30).show(truncate=False)

        valid_chunks.unpersist()

    except Exception as e:
        print(f"❌ ERROR building dictionary: {e}")
        oecd_vocabulary = None


# In[ ]:


# ==========================================
# CELL 2.5: ENSEMBLE NLP POST-PROCESSING POLISH (DRIVER-SIDE BYPASS)
# ==========================================
import pandas as pd
import spacy
import pyspark.sql.functions as F
import os

print("--- Phase 2.5: Executing Ensemble NLP Polish (Driver-Side) ---")

POLISHED_DICT_CSV_PATH = os.path.join(LOCAL_EXPORT_DIR, "oecd_dictionary_polished.csv")
USE_EXISTING_POLISHED_DICTIONARY = False

if USE_EXISTING_POLISHED_DICTIONARY and os.path.exists(POLISHED_DICT_CSV_PATH):
    print(f"Loading existing polished dictionary from {POLISHED_DICT_CSV_PATH}...")
    oecd_vocabulary = spark.read.option("header", True).csv(POLISHED_DICT_CSV_PATH) \
        .withColumn("relative_share", F.col("relative_share").cast("double")) \
        .withColumn("avg_sim", F.col("avg_sim").cast("double")) \
        .withColumn("global_count", F.col("global_count").cast("long")) \
        .withColumn("gold_sim_score", F.col("gold_sim_score").cast("double")).cache()
    print(f"✓ Loaded {oecd_vocabulary.count()} polished terms.")

else:
    if 'oecd_vocabulary' in globals() and oecd_vocabulary is not None:
        print(f"Pre-Polish Dictionary Size: {oecd_vocabulary.count()} terms")

        pdf = oecd_vocabulary.toPandas()

        print("Loading spaCy model...")
        nlp_driver = spacy.load("en_core_web_lg") 

        # TAILORED FOR DATA ENTRY: We stripped out advanced engineering/ML tools
        GOLD_STANDARD = [
            "data entry", "spreadsheet", "database", "typing", "records", 
            "crm", "erp", "clerical", "transcription", "input", "processing", 
            "accuracy", "update", "system", "log", "software", "information"
        ]

        gold_docs = [nlp_driver(g) for g in GOLD_STANDARD]

        print("Calculating similarities... (This takes about 5 seconds)")
        def get_max_sim(phrase):
            if not phrase: return 0.0
            phrase_doc = nlp_driver(str(phrase))
            return float(max([phrase_doc.similarity(g) for g in gold_docs])) if gold_docs else 0.0

        pdf['gold_sim_score'] = pdf['noun_chunk'].apply(get_max_sim)

        FINAL_SIM_THRESHOLD = 0.45 
        polished_pdf = pdf[pdf['gold_sim_score'] >= FINAL_SIM_THRESHOLD]

        # SAVE POLISHED CSV
        polished_pdf.to_csv(POLISHED_DICT_CSV_PATH, index=False)

        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
        schema = StructType([
            StructField("noun_chunk", StringType(), True),
            StructField("relative_share", DoubleType(), True),
            StructField("avg_sim", DoubleType(), True),
            StructField("global_count", LongType(), True),
            StructField("gold_sim_score", DoubleType(), True)
        ])

        polished_pdf = polished_pdf[['noun_chunk', 'relative_share', 'avg_sim', 'global_count', 'gold_sim_score']]
        clean_data_list = polished_pdf.values.tolist()

        oecd_vocabulary = spark.createDataFrame(clean_data_list, schema=schema).cache()

        print(f"Post-Polish Dictionary Size: {oecd_vocabulary.count()} terms")
        print("\n=== TOP 30 SURVIVORS (By Volume) ===")
        oecd_vocabulary.orderBy(F.col("global_count").desc()).limit(30).select("noun_chunk", "global_count", "gold_sim_score").show(truncate=False)


# In[ ]:


# ==========================================
# CELL 3: PHASE 3 - JOB CLASSIFICATION
# ==========================================
print("--- Phase 3: Classifying Occupations ---")

def run_classification_for_year(year):
    in_path = get_nlp_path(year)
    try: 
        packed_chunks = spark.read.parquet(in_path)
    except Exception as e: 
        print(f"  [Skipping {year}] - Could not read data: {e}")
        return None

    if oecd_vocabulary is None: return None

    # Explode and filter
    year_chunks = packed_chunks.select(
        "doc_JobID", "doc_BGTOcc",
        F.explode(F.arrays_zip("noun_chunks", "sim_scores")).alias("zipped")
    ).select(
        "doc_JobID", "doc_BGTOcc",
        F.col("zipped.noun_chunks").alias("noun_chunk"),
        F.col("zipped.sim_scores").alias("sim_data")
    ).filter(F.col("sim_data") >= SIM_GROUNDING)

    # Tag jobs based on dictionary presence
    tagged_chunks = year_chunks.join(F.broadcast(oecd_vocabulary), "noun_chunk", "inner")

    job_scores = tagged_chunks.groupBy("doc_JobID", "doc_BGTOcc") \
        .agg(F.countDistinct("noun_chunk").alias("unique_data_terms")) \
        .withColumnRenamed("doc_BGTOcc", "soc_2020")

    # Regex Armor Applied Here
    job_scores = job_scores.withColumn("soc4", F.regexp_extract("soc_2020", r"(\d{4})", 1)).filter(F.col("soc4") != "")
    is_intensive = (F.col("unique_data_terms") >= DATA_THRESHOLD)

    classified = job_scores \
        .withColumn("data_entry", (F.col("soc4").isin(SOC_GROUPS["data_entry"]) & is_intensive).cast("int")) \
        .withColumn("database", (F.col("soc4").isin(SOC_GROUPS["database"]) & is_intensive).cast("int")) \
        .withColumn("data_analytics", (F.col("soc4").isin(SOC_GROUPS["analytics"]) & is_intensive).cast("int")) \
        .withColumn("any_data_intensive", is_intensive.cast("int"))

    # METHODOLOGY NOTE: We use ALL jobs here (including 0 valid chunks) because 
    # the Phase 4 Sector mapping requires the entire economy to match the SUT Tables.
    all_jobs_year = packed_chunks.select("doc_JobID", F.col("doc_BGTOcc").alias("soc_2020")).distinct() \
        .withColumn("soc4", F.regexp_extract("soc_2020", r"(\d{4})", 1)).filter(F.col("soc4") != "")

    merged = all_jobs_year.join(classified, ["doc_JobID", "soc4", "soc_2020"], "left").fillna(0)

    occ_sum = merged.groupBy("soc4").agg(
        F.count("*").alias("total_jobs"),
        F.sum("data_entry").alias("data_entry_jobs"),
        F.sum("database").alias("database_jobs"),
        F.sum("data_analytics").alias("data_analytics_jobs"),
        F.sum("any_data_intensive").alias("any_data_intensive_jobs")
    ).withColumn("year", F.lit(year))

    occ_sum = occ_sum \
        .withColumn("total_data_share", 100 * F.col("any_data_intensive_jobs") / F.col("total_jobs")) \
        .withColumn("data_entry_share", 100 * F.col("data_entry_jobs") / F.col("total_jobs")) \
        .withColumn("database_share", 100 * F.col("database_jobs") / F.col("total_jobs")) \
        .withColumn("data_analytics_share", 100 * F.col("data_analytics_jobs") / F.col("total_jobs"))

    return occ_sum

occupation_summaries = {y: run_classification_for_year(y) for y in YEARS}
print("✓ Occupations Classified.")


# In[ ]:


# ==========================================
# CELL 3.5: EXPORT JOB-LEVEL CLASSIFICATIONS (DATA INTENSIVE ONLY)
# ==========================================
import os
import pyspark.sql.functions as F

print("--- Phase 3.5: Exporting Data-Intensive Job IDs ---")

# Uses the current working directory, works on anyone's machine
EXPORT_FILE = os.path.join(LOCAL_EXPORT_DIR, "job_level_export_data_entry_ONLY.csv")

if 'oecd_vocabulary' in globals() and oecd_vocabulary is not None:
    all_years_df = None

    for year in YEARS:
        in_path = get_nlp_path(year)
        try: 
            packed_chunks = spark.read.parquet(in_path)
        except: 
            continue

        year_chunks = packed_chunks.select(
            "doc_JobID", "doc_BGTOcc",
            F.explode(F.arrays_zip("noun_chunks", "sim_scores")).alias("zipped")
        ).select(
            "doc_JobID", "doc_BGTOcc",
            F.col("zipped.noun_chunks").alias("noun_chunk"),
            F.col("zipped.sim_scores").alias("sim_data")
        ).filter(F.col("sim_data") >= SIM_GROUNDING)

        tagged_chunks = year_chunks.join(F.broadcast(oecd_vocabulary), "noun_chunk", "inner")

        job_scores = tagged_chunks.groupBy("doc_JobID", "doc_BGTOcc") \
            .agg(F.countDistinct("noun_chunk").alias("unique_data_terms"))

        is_intensive = (F.col("unique_data_terms") >= DATA_THRESHOLD).cast("int")

        classified_jobs = job_scores.select(
            F.lit(year).alias("year"),
            F.col("doc_JobID"),
            F.regexp_extract("doc_BGTOcc", r"(\d{4})", 1).alias("soc4"),
            is_intensive.alias("is_data_intensive_job")
        ).filter(
            (F.col("soc4") != "") & 
            (F.col("is_data_intensive_job") == 1) 
        )

        if all_years_df is None:
            all_years_df = classified_jobs
        else:
            all_years_df = all_years_df.unionByName(classified_jobs)

    if all_years_df is not None:
        total_jobs = all_years_df.count()
        print(f"Exporting {total_jobs} pure Data-Intensive jobs to CSV...")

        pdf_export = all_years_df.toPandas()
        pdf_export.to_csv(EXPORT_FILE, index=False)
        print(f"✓ Filtered Job-Level Data Exported Successfully to: {EXPORT_FILE}")
else:
    print("Cannot export: Dictionary not found.")


# In[ ]:


# ==========================================
# CELL 4: PHASE 4 - SECTOR MAPPING
# ==========================================
print("--- Phase 4: Applying Census Weights ---")

df_census = spark.read.option("header", True).csv(CENSUS_CSV)
desc_col = df_census.columns[0]
df_census = df_census.withColumn("soc4", F.regexp_extract(F.col(desc_col), r"^(\d{4})", 1))

sic_cols = [c for c in df_census.columns if c != desc_col and c != "soc4"]
stack_expr = f"stack({len(sic_cols)}, " + ", ".join([f"'{c}', `{c}`" for c in sic_cols]) + ") as (sic_col, count_raw)"
long_df = df_census.select("soc4", F.expr(stack_expr))

long_df = long_df.withColumn("sic2", F.regexp_extract("sic_col", r"^(\d{2})", 1).cast("int")) \
                 .withColumn("n", F.regexp_replace("count_raw", ",", "").cast("long")).filter(F.col("n") > 0)

long_df = long_df.withColumn("SIC_Code", F.expr("""
    CASE WHEN sic2 BETWEEN 1 AND 3 THEN 'A' WHEN sic2 BETWEEN 5 AND 39 THEN 'B-E' WHEN sic2 BETWEEN 41 AND 43 THEN 'F' WHEN sic2 BETWEEN 45 AND 56 THEN 'G-I' WHEN sic2 BETWEEN 58 AND 63 THEN 'J' WHEN sic2 BETWEEN 64 AND 66 THEN 'K' WHEN sic2 = 68 THEN 'L' WHEN sic2 BETWEEN 69 AND 82 THEN 'M-N' WHEN sic2 BETWEEN 84 AND 88 THEN 'O-Q' WHEN sic2 BETWEEN 90 AND 98 THEN 'R-T' WHEN sic2 = 99 THEN 'U' ELSE NULL END
""")).filter(F.col("SIC_Code").isNotNull())

totals = long_df.groupBy("soc4").agg(F.sum("n").alias("total_soc"))
weights_df = long_df.groupBy("soc4", "SIC_Code").agg(F.sum("n").alias("n_sic")) \
                 .join(totals, "soc4").withColumn("w_soc4_SIC", F.col("n_sic") / F.col("total_soc")).cache()

sector_summaries = []
for year, occ_df in occupation_summaries.items():
    if occ_df is None: continue

    joined = occ_df.join(weights_df, "soc4", "left").fillna(0, subset=["w_soc4_SIC"])

    weighted = joined.select("SIC_Code",
        (F.col("total_jobs") * F.col("w_soc4_SIC")).alias("w_total"),
        (F.col("data_entry_jobs") * F.col("w_soc4_SIC")).alias("w_entry"),
        (F.col("database_jobs") * F.col("w_soc4_SIC")).alias("w_db"),
        (F.col("data_analytics_jobs") * F.col("w_soc4_SIC")).alias("w_ana"),
        (F.col("any_data_intensive_jobs") * F.col("w_soc4_SIC")).alias("w_any")
    )

    sec_sum = weighted.groupBy("SIC_Code").agg(
        F.sum("w_total").alias("total_jobs"), F.sum("w_entry").alias("data_entry_jobs"),
        F.sum("w_db").alias("database_jobs"), F.sum("w_ana").alias("data_analytics_jobs"), 
        F.sum("w_any").alias("any_data_intensive_jobs")
    )

    sec_sum = sec_sum \
        .withColumn("total_data_share", F.when(F.col("total_jobs") > 0, 100 * F.col("any_data_intensive_jobs") / F.col("total_jobs")).otherwise(0.0)) \
        .withColumn("data_entry_share", F.when(F.col("total_jobs") > 0, 100 * F.col("data_entry_jobs") / F.col("total_jobs")).otherwise(0.0)) \
        .withColumn("database_share", F.when(F.col("total_jobs") > 0, 100 * F.col("database_jobs") / F.col("total_jobs")).otherwise(0.0)) \
        .withColumn("data_analytics_share", F.when(F.col("total_jobs") > 0, 100 * F.col("data_analytics_jobs") / F.col("total_jobs")).otherwise(0.0)) \
        .withColumn("year", F.lit(year))

    sector_summaries.append(sec_sum)

print("✓ Sectors Mapped.")


# In[ ]:


# ==========================================
# CELL 5: PHASE 5 - ECONOMIC VALUATION (4 SCENARIOS)
# ==========================================
print("--- Phase 5: Calculating Economic Valuation ---")

if not sector_summaries: raise ValueError("No sector data generated. Check Cell 3 output.")
full_sector_df = sector_summaries[0]
for d in sector_summaries[1:]: full_sector_df = full_sector_df.unionByName(d)

sut_df = spark.read.option("header", True).csv(SUT_CSV).filter(F.col("year") == SUT_YEAR) \
    .select(
        F.upper(F.trim("SIC_Code")).alias("SIC_Code"), 
        (F.col("GVA_basic_prices").cast("double") * 1000000).alias("GVA_basic_prices"), 
        (F.col("COMP_EMP").cast("double") * 1000000).alias("COMP_EMP")
    )

alpha_expr = F.create_map([F.lit(x) for i in ALPHA_MAP.items() for x in i])
valued = full_sector_df.withColumn("SIC_Code", F.upper(F.trim("SIC_Code"))) \
    .join(sut_df, "SIC_Code", "inner") \
    .withColumn("alpha_low", F.lit(ALPHA_LOW)) \
    .withColumn("alpha_avg", F.lit(ALPHA_ECONOMY_AVG)) \
    .withColumn("alpha_sector", F.coalesce(alpha_expr[F.col("SIC_Code")], F.lit(ALPHA_ECONOMY_AVG)))

# SCENARIO 1: Sector Alpha Map
valued = valued.withColumn("total_investment_sector", F.col("alpha_sector") * F.col("COMP_EMP") * (F.col("total_data_share")/100))
# SCENARIO 2: Economy Average (3.62)
valued = valued.withColumn("inv_avg_tot", F.col("alpha_avg") * F.col("COMP_EMP") * (F.col("total_data_share")/100))
# SCENARIO 3: Conservative (1.58)
valued = valued.withColumn("inv_low_tot", F.col("alpha_low") * F.col("COMP_EMP") * (F.col("total_data_share")/100))
# SCENARIO 4: Raw Wages (No Alpha, flat 1.0)
valued = valued.withColumn("inv_raw_wages", F.lit(1.0) * F.col("COMP_EMP") * (F.col("total_data_share")/100))

# INTENSITY PERCENTAGES
valued = valued \
    .withColumn("inv_share_gva_sector", F.when(F.col("GVA_basic_prices")>0, (F.col("total_investment_sector")/F.col("GVA_basic_prices"))*100).otherwise(0.0)) \
    .withColumn("inv_share_gva_avg", F.when(F.col("GVA_basic_prices")>0, (F.col("inv_avg_tot")/F.col("GVA_basic_prices"))*100).otherwise(0.0)) \
    .withColumn("inv_share_gva_low", F.when(F.col("GVA_basic_prices")>0, (F.col("inv_low_tot")/F.col("GVA_basic_prices"))*100).otherwise(0.0)) \
    .withColumn("inv_share_gva_raw", F.when(F.col("GVA_basic_prices")>0, (F.col("inv_raw_wages")/F.col("GVA_basic_prices"))*100).otherwise(0.0))

valued.cache()
print("✓ Valuation Complete.")


# In[ ]:


# ==========================================
# CELL 6a: SETUP & DICTIONARY WORD CLOUD
# ==========================================
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from wordcloud import WordCloud

print("--- Phase 6: Generating Visualizations ---")

if 'oecd_vocabulary' in globals() and oecd_vocabulary is not None:
    vocab_pdf = oecd_vocabulary.toPandas()
    if not vocab_pdf.empty:
        print("\nGenerating Dictionary Word Cloud...")

        # THE FIX: Strip out any hidden newline or return characters that crash the image library
        vocab_pdf['noun_chunk'] = vocab_pdf['noun_chunk'].astype(str).str.replace(r'\n|\r', ' ', regex=True)

        # Dictionary linking words to their Relative Share importance
        word_freq = dict(zip(vocab_pdf['noun_chunk'], vocab_pdf['relative_share']))

        wc = WordCloud(width=1200, height=500, background_color='white', colormap='viridis', max_words=100)
        wc.generate_from_frequencies(word_freq)

        plt.figure(figsize=(16, 8))
        plt.imshow(wc, interpolation='bilinear')
        plt.axis('off')
        plt.title("OECD Data Dictionary (Sized by Specificity to Data-Work)", fontsize=22, pad=20)
        plt.tight_layout()
        plt.show()
    else:
        print("Dictionary is empty. Cannot generate Word Cloud.")


# In[ ]:


# ==========================================
# CELL 6b: TOTAL UK GVA vs. DATA INVESTMENT (LINE CHART)
# ==========================================
from plotly.subplots import make_subplots

pdf = valued.toPandas()

if pdf.empty:
    print("WARNING: Valuation DataFrame is empty. Cannot plot economic charts.")
else:
    pdf['year_str'] = pdf['year'].astype(str)
    econ = pdf.groupby("year_str")[["inv_raw_wages", "inv_low_tot", "inv_avg_tot", "total_investment_sector", "GVA_basic_prices"]].sum().reset_index()

    fig_macro_abs = make_subplots(specs=[[{"secondary_y": True}]])

    def fmt_b(vals): return [f"£{v/1e9:.1f}B" for v in vals]
    def fmt_t(vals): return [f"£{v/1e12:.2f}T" for v in vals]

    fig_macro_abs.add_trace(go.Scatter(
        x=econ['year_str'], y=econ['total_investment_sector'], name='Sector Map (Upper Bound)',
        mode='lines+markers+text', text=fmt_b(econ['total_investment_sector']), textposition="top center",
        line=dict(color='#00008b', width=4), marker=dict(size=10)
    ), secondary_y=False)

    fig_macro_abs.add_trace(go.Scatter(
        x=econ['year_str'], y=econ['inv_avg_tot'], name=f'Economy Average (α={ALPHA_ECONOMY_AVG})',
        mode='lines+markers+text', text=fmt_b(econ['inv_avg_tot']), textposition="top center",
        line=dict(color='#2ca02c', width=4, dash='dash'), marker=dict(size=10)
    ), secondary_y=False)

    fig_macro_abs.add_trace(go.Scatter(
        x=econ['year_str'], y=econ['inv_low_tot'], name=f'Conservative (α={ALPHA_LOW})',
        mode='lines+markers+text', text=fmt_b(econ['inv_low_tot']), textposition="bottom center",
        line=dict(color='#a6cee3', width=4), marker=dict(size=10)
    ), secondary_y=False)

    # NEW: Raw Wages
    fig_macro_abs.add_trace(go.Scatter(
        x=econ['year_str'], y=econ['inv_raw_wages'], name='Raw Wages (No Alpha)',
        mode='lines+markers', line=dict(color='#ff7f0e', width=3, dash='dot'), marker=dict(size=8)
    ), secondary_y=False)

    fig_macro_abs.add_trace(go.Scatter(
        x=econ['year_str'], y=econ['GVA_basic_prices'], name='Total UK Economy GVA',
        mode='lines+markers+text', text=fmt_t(econ['GVA_basic_prices']), textposition="top right",
        line=dict(color='#A9A9A9', width=3, dash='dot'), marker=dict(size=8)
    ), secondary_y=True)

    fig_macro_abs.update_layout(
        title="Total UK Gross Value Added (GVA) vs. Data Investment Scenarios",
        template="plotly_white", height=700, legend=dict(x=0.01, y=0.99, bgcolor="rgba(255,255,255,0.8)")
    )
    fig_macro_abs.update_yaxes(title_text="Data Investment (Billions)", secondary_y=False, range=[0, econ['total_investment_sector'].max() * 1.5])
    fig_macro_abs.update_yaxes(title_text="Total GVA (Trillions)", secondary_y=True, showgrid=False, range=[0, econ['GVA_basic_prices'].max() * 1.3])
    fig_macro_abs.show()


# In[ ]:


# ==========================================
# CELL 6c: GVA vs. DATA INVESTMENT BY SECTOR
# ==========================================
if not pdf.empty:
    fig_sec_abs = px.bar(
        pdf, x="year_str", y=["GVA_basic_prices", "total_investment_sector"],
        facet_col="SIC_Code", facet_col_wrap=4, barmode="group",
        title="GVA vs. Data Investment by Industry Sector (Absolute £)",
        labels={'value': 'Absolute Value', 'variable': 'Metric', 'year_str': 'Year'},
        template="plotly_white", height=800,
        color_discrete_map={"GVA_basic_prices": "#E5E5E5", "total_investment_sector": "#1f77b4"}
    )

    newnames = {'GVA_basic_prices': 'Total Sector GVA', 'total_investment_sector': 'Data Investment (Sector Alpha)'}
    fig_sec_abs.for_each_trace(lambda t: t.update(name = newnames[t.name],
                                                  legendgroup = newnames[t.name],
                                                  hovertemplate = t.hovertemplate.replace(t.name, newnames[t.name])))

    fig_sec_abs.update_yaxes(matches=None, tickprefix="£") 

    # THE FIX: This forces the years to render under every single subplot
    fig_sec_abs.update_xaxes(showticklabels=True) 

    fig_sec_abs.show()


# In[ ]:


# ==========================================
# CELL 6d: DATA INVESTMENT AS % OF UK ECONOMY
# ==========================================
if not pdf.empty:
    econ["Raw Wages %"] = (econ["inv_raw_wages"] / econ["GVA_basic_prices"]) * 100
    econ["Conservative %"] = (econ["inv_low_tot"] / econ["GVA_basic_prices"]) * 100
    econ["Economy Avg %"] = (econ["inv_avg_tot"] / econ["GVA_basic_prices"]) * 100
    econ["Sector Map %"] = (econ["total_investment_sector"] / econ["GVA_basic_prices"]) * 100

    fig_macro_pct = px.line(
        econ, x="year_str", y=["Sector Map %", "Economy Avg %", "Conservative %", "Raw Wages %"], 
        title="Data Investment as a Percentage of Total UK Economy (GVA)", 
        markers=True, template="plotly_white",
        labels={'value': '% of Total GVA', 'variable': 'Valuation Scenario', 'year_str': 'Year'},
        color_discrete_sequence=["#00008b", "#2ca02c", "#a6cee3", "#ff7f0e"]
    )

    fig_macro_pct.update_traces(line=dict(width=4), marker=dict(size=10))
    fig_macro_pct.update_yaxes(ticksuffix=" %")
    fig_macro_pct.show()



# In[ ]:


# ==========================================
# CELL 6e: DATA INVESTMENT AS % OF SECTOR GVA (4 SCENARIOS)
# ==========================================
if not pdf.empty:
    def plot_sector_intensity(y_col, title_suffix):
        fig = px.line(
            pdf, x="year_str", y=y_col, color="SIC_Code",
            title=f"Data Investment Intensity by Sector ({title_suffix})",
            labels={y_col: '% of Sector GVA', 'SIC_Code': 'Industry (SIC)', 'year_str': 'Year'},
            markers=True, template="plotly_white", height=500
        )
        fig.update_traces(line=dict(width=3), marker=dict(size=8))
        fig.update_yaxes(ticksuffix=" %")
        fig.show()

    plot_sector_intensity("inv_share_gva_sector", "Sector-Specific Alpha Markup")
    plot_sector_intensity("inv_share_gva_avg", f"Economy Average Alpha: {ALPHA_ECONOMY_AVG}")
    plot_sector_intensity("inv_share_gva_low", f"Conservative Alpha: {ALPHA_LOW}")
    plot_sector_intensity("inv_share_gva_raw", "Raw Wages (No Alpha)")


# In[ ]:


# ==========================================
# CELL 6f: DATA INTENSITY BY INDUSTRY (HEATMAP)
# ==========================================
if not pdf.empty:
    # Pivot the data for a heatmap (Rows = SIC, Columns = Year, Values = Data Share)
    heatmap_data = pdf.pivot(index='SIC_Code', columns='year_str', values='total_data_share')

    fig_heat = px.imshow(
        heatmap_data, 
        title="Heatmap: Percentage of Workforce in Data-Intensive Roles by Sector",
        labels=dict(x="Year", y="Industry Sector (SIC)", color="% of Workforce"),
        color_continuous_scale="Blues", aspect="auto", template="plotly_white", height=600
    )

    fig_heat.update_traces(text=heatmap_data.round(2).astype(str) + "%", texttemplate="%{text}")
    fig_heat.show()


# In[ ]:


# ==========================================
# CELL 6g: OVERALL WORKFORCE DATA INTENSITY TREND
# ==========================================
if not pdf.empty:
    intensity_df = pdf.groupby("year_str")[["any_data_intensive_jobs", "total_jobs"]].sum().reset_index()
    intensity_df["Overall Intensity %"] = (intensity_df["any_data_intensive_jobs"] / intensity_df["total_jobs"]) * 100

    fig_intensity = px.line(
        intensity_df, x="year_str", y="Overall Intensity %", 
        title="UK Workforce Trend: Percentage of All Jobs Requiring Data Skills", 
        markers=True, template="plotly_white", color_discrete_sequence=["#2ca02c"]
    )

    fig_intensity.update_traces(line=dict(width=4), marker=dict(size=12))
    fig_intensity.update_yaxes(ticksuffix=" %")
    fig_intensity.update_layout(xaxis_title="Year", yaxis_title="% of Total UK Jobs")
    fig_intensity.show()


# In[ ]:


# ==========================================
# CELL 6h: TOP 50 OCCUPATIONS BY YEAR (PERCENTAGE RANK CHART)
# ==========================================
occ_frames = [df.toPandas() for y, df in occupation_summaries.items() if df is not None]

if occ_frames:
    all_occ_df = pd.concat(occ_frames, ignore_index=True)
    if not all_occ_df.empty:
        all_occ_df['soc4'] = all_occ_df['soc4'].astype(str)
        all_occ_df['year_str'] = all_occ_df['year'].astype(str)
        years_to_plot = sorted(all_occ_df['year_str'].unique())

        for plot_year in years_to_plot:
            year_data = all_occ_df[all_occ_df['year_str'] == plot_year]

            # THE FIX: Grab Top 50, sort ascending so the highest ends up at the top of Plotly's layout
            top_50 = year_data.nlargest(50, "total_data_share").sort_values("total_data_share", ascending=True)

            melted = top_50.melt(
                id_vars=["soc4"], 
                value_vars=["data_entry_share", "database_share", "data_analytics_share"],
                var_name="Category", value_name="Share"
            )
            melted["Category"] = melted["Category"].map({
                "data_entry_share": "Data Entry", 
                "database_share": "Database", 
                "data_analytics_share": "Data Analytics"
            })

            fig_soc = px.bar(
                melted, x="Share", y="soc4", color="Category", orientation='h',
                title=f"Top 50 Data-Intensive Occupations in {plot_year} (Intensity %)",
                labels={'Share': '% of Job Adverts', 'soc4': 'SOC Code'},
                template="plotly_white", barmode="stack", height=1200, # Increased height for 50 items
                color_discrete_map={"Data Entry": "#a6cee3", "Database": "#1f77b4", "Data Analytics": "#00008b"}
            )

            fig_soc.update_xaxes(ticksuffix=" %")
            fig_soc.update_layout(yaxis=dict(type='category', dtick=1)) 
            fig_soc.show()



# In[ ]:


# ==========================================
# CELL 7: PHASE 7 - AUDIT LOG EXPORT
# ==========================================
import datetime

LOG_FILE = "pipeline_audit_log_FINAL.txt"

def dump_df(df, name, f, limit=50):
    f.write(f"\n{'='*50}\nDATASET: {name}\n{'='*50}\n")
    if df is None:
        f.write("[MISSING OR EMPTY]\n")
        return
    f.write(f"Columns: {df.columns}\n")
    try:
        f.write(df.limit(limit).toPandas().to_string())
        f.write("\n")
    except Exception as e:
        f.write(f"Error dumping: {e}\n")

with open(LOG_FILE, "w") as f:
    f.write(f"OECD PIPELINE FINAL AUDIT - {datetime.datetime.now()}\n")

    if 'oecd_vocabulary' in globals() and oecd_vocabulary is not None:
        top_vocab = oecd_vocabulary.orderBy(F.col("relative_share").desc())

        # NEW: Print Top 30 to console for immediate reviewer sanity-check
        print("\n=== TOP 30 OECD DICTIONARY TERMS ===")
        try:
            print(top_vocab.limit(30).toPandas().to_string(index=False))
        except:
            print("Dictionary is empty.")
        print("====================================\n")

        dump_df(top_vocab, "Learned OECD Vocabulary", f, limit=50)

    dump_df(weights_df, "Census Weights", f)

    sample_year = 2024
    f.write(f"\n\n>>> YEAR {sample_year} SNAPSHOTS <<<\n")

    if sample_year in occupation_summaries and occupation_summaries[sample_year] is not None:
        dump_df(occupation_summaries[sample_year], f"Occupation Summary {sample_year}", f)

    sec_2024 = next((sec for sec in sector_summaries if sec.filter(F.col("year") == sample_year).count() > 0), None)
    dump_df(sec_2024, f"Sector Summary {sample_year}", f)
    dump_df(valued, "Final Valuation Table", f)

print(f"Data successfully dumped to: {LOG_FILE}")


# In[ ]:


# ==========================================
# CELL 6i: DATA VOLUME BY INDUSTRY (HEATMAP - ABSOLUTE COUNTS)
# ==========================================
if not pdf.empty:
    # Pivot the data for a heatmap (Rows = SIC, Columns = Year, Values = Absolute Job Count)
    heatmap_counts = pdf.pivot(index='SIC_Code', columns='year_str', values='any_data_intensive_jobs')

    fig_heat_counts = px.imshow(
        heatmap_counts, 
        title="Heatmap: Total Volume of Data-Intensive Job Adverts by Sector",
        labels=dict(x="Year", y="Industry Sector (SIC)", color="Job Count"),
        color_continuous_scale="Blues", 
        aspect="auto", 
        template="plotly_white", 
        height=600,
        text_auto=",.0f"  # Automatically formats numbers with commas and no decimals
    )

    fig_heat_counts.show()


# In[ ]:


# ==========================================
# CELL 6j: TOP 50 OCCUPATIONS BY YEAR (ABSOLUTE COUNTS RANK CHART)
# ==========================================
if occ_frames:
    if not all_occ_df.empty:
        for plot_year in years_to_plot:
            year_data = all_occ_df[all_occ_df['year_str'] == plot_year]

            # THE FIX: Grab Top 50 by Volume, sorted ascending
            top_50_vol = year_data.nlargest(50, "any_data_intensive_jobs").sort_values("any_data_intensive_jobs", ascending=True)

            melted_vol = top_50_vol.melt(
                id_vars=["soc4"], 
                value_vars=["data_entry_jobs", "database_jobs", "data_analytics_jobs"],
                var_name="Category", value_name="Job Count"
            )
            melted_vol["Category"] = melted_vol["Category"].map({
                "data_entry_jobs": "Data Entry", 
                "database_jobs": "Database", 
                "data_analytics_jobs": "Data Analytics"
            })

            fig_soc_counts = px.bar(
                melted_vol, x="Job Count", y="soc4", color="Category", orientation='h',
                title=f"Top 50 Occupations by Absolute Data Jobs in {plot_year}",
                labels={'Job Count': 'Number of Data Jobs', 'soc4': 'SOC Code'},
                template="plotly_white", barmode="stack", height=1200,
                color_discrete_map={"Data Entry": "#a6cee3", "Database": "#1f77b4", "Data Analytics": "#00008b"}
            )

            fig_soc_counts.update_layout(
                yaxis=dict(type='category', dtick=1),
                xaxis=dict(tickformat=",.0f")
            ) 
            fig_soc_counts.show()


# In[ ]:


# ==========================================
# CELL 6k: ALPHA SENSITIVITY RANGE (OECD SCENARIOS)
# ==========================================
import pandas as pd
import plotly.graph_objects as go

print("Generating Alpha Sensitivity Chart...")

# Build a Pandas DataFrame directly from your configuration variables
alpha_data = []
for sector, alpha_val in ALPHA_MAP.items():
    alpha_data.append({"Sector": sector, "Alpha": alpha_val, "Scenario": "Upper Bound (UK-Specific)"})
    alpha_data.append({"Sector": sector, "Alpha": ALPHA_LOW, "Scenario": "Lower Bound (Conservative)"})

alpha_df = pd.DataFrame(alpha_data)

# Create a grouped bar chart
fig_alpha = px.bar(
    alpha_df, x="Sector", y="Alpha", color="Scenario", barmode="group",
    title="Alpha (α) Capital Markup: Lower Bound vs. Upper Bound Scenarios",
    labels={"Alpha": "Multiplier (α)", "Sector": "Industry Sector (SIC)"},
    template="plotly_white", height=600,
    color_discrete_map={
        "Upper Bound (UK-Specific)": "#1f77b4", 
        "Lower Bound (Conservative)": "#a6cee3"
    }
)

# Add a horizontal line to show the Economy-Wide Average
fig_alpha.add_hline(
    y=ALPHA_ECONOMY_AVG, line_dash="dot", line_color="black", 
    annotation_text=f"Economy-Wide Average ({ALPHA_ECONOMY_AVG})", 
    annotation_position="top left"
)

fig_alpha.update_layout(yaxis=dict(ticksuffix="x"))
fig_alpha.show()


# In[ ]:


# ==========================================
# CELL 6l: DATA INTENSITY BY TOP 20 SOC CODES (PERCENTAGE HEATMAP)
# ==========================================
if occ_frames and not all_occ_df.empty:
    # 1. Find the top 20 SOCs by their average intensity across all years
    soc_avg_pct = all_occ_df.groupby('soc4')['total_data_share'].mean().reset_index()
    top_20_socs_pct = soc_avg_pct.nlargest(20, 'total_data_share')['soc4'].tolist()

    # 2. Filter and pivot
    filtered_pct = all_occ_df[all_occ_df['soc4'].isin(top_20_socs_pct)]
    heatmap_soc_pct = filtered_pct.pivot(index='soc4', columns='year_str', values='total_data_share')

    fig_heat_soc_pct = px.imshow(
        heatmap_soc_pct, 
        title="Heatmap: Data-Intensity Percentage by Top 20 Occupations",
        labels=dict(x="Year", y="SOC 2020 Code", color="% of Adverts"),
        color_continuous_scale="Purples", aspect="auto", template="plotly_white", height=700,
        text_auto=".1f"
    )
    fig_heat_soc_pct.update_yaxes(type='category')
    fig_heat_soc_pct.show()


# In[ ]:


# ==========================================
# CELL 6m: DATA VOLUME BY TOP 20 SOC CODES (ABSOLUTE COUNT HEATMAP)
# ==========================================
if occ_frames and not all_occ_df.empty:
    # 1. Find the top 20 SOCs by their average absolute volume
    soc_avg_vol = all_occ_df.groupby('soc4')['any_data_intensive_jobs'].mean().reset_index()
    top_20_socs_vol = soc_avg_vol.nlargest(20, 'any_data_intensive_jobs')['soc4'].tolist()

    # 2. Filter and pivot
    filtered_vol = all_occ_df[all_occ_df['soc4'].isin(top_20_socs_vol)]
    heatmap_soc_vol = filtered_vol.pivot(index='soc4', columns='year_str', values='any_data_intensive_jobs')

    fig_heat_soc_vol = px.imshow(
        heatmap_soc_vol, 
        title="Heatmap: Absolute Volume of Data Jobs by Top 20 Occupations",
        labels=dict(x="Year", y="SOC 2020 Code", color="Job Count"),
        color_continuous_scale="Oranges", aspect="auto", template="plotly_white", height=700,
        text_auto=",.0f"
    )
    fig_heat_soc_vol.update_yaxes(type='category')
    fig_heat_soc_vol.show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[55]:


#converting the script to using the nbconvert
get_ipython().system('jupyter nbconvert --to script Test_1.5.ipynb --output oecd_pipeline_final')


# In[ ]:





# In[ ]:


# #I need this as a chart
# 3.4 The Sensitivity Range (OECD Scenarios)
# Because the Alpha markup is a macroeconomic estimate, we present our final findings as a range to provide stakeholders with a transparent "Confidence Band":

# Lower Bound (Conservative): A flat multiplier of 1.58 applied to all sectors, acting as an internationally recognized benchmark (utilized by Statistics Canada) representing the absolute minimum capital footprint of an office worker.

# Upper Bound (UK-Specific): Our calculated, highly specific UK markups derived directly from the 2025 Blue Book (ranging from 2.07 up to 6.65, with outliers capped at 3.62).


# In[ ]:




