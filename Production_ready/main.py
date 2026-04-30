# ==========================================
# main.py - OECD PIPELINE EXECUTOR
# ==========================================
import os
import re
import spacy
import pandas as pd
import logging
import datetime
import base64
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from wordcloud import WordCloud
from tqdm import tqdm

# Import our unified configuration
import config

# --- SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

os.makedirs(config.EXPORT_DIR, exist_ok=True)
os.makedirs(config.REPORTS_DIR, exist_ok=True) 

print("\n" + "="*50)
print("STARTING DYNAMIC OECD MACROECONOMIC PIPELINE")
print("="*50 + "\n")

# --- SPARK INITIALIZATION (LOCAL LAPTOP MODE) ---
print("Initializing Spark Cluster (Local Testing Mode)...")
spark = SparkSession.builder.appName("OECD_Pipeline_Local") \
    .master("local[4]") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# ==========================================
# PHASE 0: PRE-FLIGHT CLEANUP (ANTI-GHOSTING)
# ==========================================
print("\n--- PHASE 0: Pre-Flight Environment Cleanup ---")

# 1. Flush the Spark Cluster RAM
spark.catalog.clearCache()
print("  -> Spark memory cache explicitly cleared.")

# 2. Delete lingering CSVs to prevent overlapping file states
if not config.USE_EXISTING_DICTIONARY:
    if os.path.exists(config.DICT_CSV_PATH):
        os.remove(config.DICT_CSV_PATH)
        print("  -> Deleted old Raw Dictionary.")

if not config.USE_EXISTING_POLISHED_DICTIONARY:
    if os.path.exists(config.POLISHED_DICT_CSV_PATH):
        os.remove(config.POLISHED_DICT_CSV_PATH)
        print("  -> Deleted old Polished Dictionary.")

        
# ==========================================
# PHASE 1: NLP EXTRACTION
# ==========================================
print("\n--- PHASE 1: NLP Extraction & Regex Cleaning ---")
noun_schema_packed = StructType([
    StructField("doc_JobID", StringType()), StructField("doc_BGTOcc", StringType()), 
    StructField("doc_year", IntegerType()), StructField("doc_month", IntegerType()),
    StructField("noun_chunks", ArrayType(StringType())), StructField("sim_scores", ArrayType(DoubleType()))
])

def extract_noun_chunks_packed(iterator):
    try: nlp = spacy.load("en_core_web_lg", disable=["lemmatizer", "ner"])
    except: nlp = spacy.load("en_core_web_sm")
    target = nlp("data")

    for pdf in iterator:
        rows = []
        texts = pdf["full_text"].fillna("").astype(str).tolist()
        meta = list(zip(pdf["job_id"].astype(str), pdf["soc_2020"].astype(str), pdf["doc_year"], pdf["doc_month"]))

        for i, doc in enumerate(nlp.pipe(texts, batch_size=50)):
            chunks_list, sims_list = [], []
            for chunk in doc.noun_chunks:
                if chunk.has_vector:
                    raw_text = chunk.text.lower()
                    cleaned = re.sub(r'[^a-z\s]', ' ', raw_text)
                    cleaned = " ".join(cleaned.split())
                    word_count = len(cleaned.split())
                    
                    if cleaned and 1 <= word_count <= 4:
                        chunks_list.append(cleaned)
                        sims_list.append(float(chunk.similarity(target)))

            rows.append({
                'doc_JobID': meta[i][0], 'doc_BGTOcc': meta[i][1],
                'doc_year': meta[i][2], 'doc_month': meta[i][3],
                'noun_chunks': chunks_list, 'sim_scores': sims_list
            })
        yield pd.DataFrame(rows) if rows else pd.DataFrame(columns=noun_schema_packed.fieldNames())

for year in config.YEARS:
    out_path = config.get_nlp_path(year)
    if not config.FORCE_RECOMPUTE_NLP:
        try:
            if spark.read.parquet(out_path).limit(1).count() > 0:
                print(f"  [SKIP] {year} already extracted.")
                continue
        except: pass

    try:
        print(f"  -> Extracting Data for {year}...")
        df_raw = spark.read.parquet(config.PARQUET_SOURCE).withColumn("date", F.to_date("date")).filter(F.year("date") == year).withColumn("doc_year", F.year("date")).withColumn("doc_month", F.month("date"))
        if config.SAMPLE_FRACTION < 1.0: df_raw = df_raw.sample(False, config.SAMPLE_FRACTION, seed=42)
        if df_raw.count() == 0: continue
        
        df_raw = df_raw.repartition(max(10, int(df_raw.count()/10000)))
        chunks = df_raw.mapInPandas(extract_noun_chunks_packed, schema=noun_schema_packed)
        chunks.write.mode("overwrite").partitionBy("doc_month").parquet(out_path)
    except Exception as e: print(f"  [ERROR] {year}: {e}")

# ==========================================
# PHASE 2: OECD DICTIONARY (DYNAMIC)
# ==========================================
print("\n--- PHASE 2: OECD Dictionary ---")
if config.USE_EXISTING_DICTIONARY and os.path.exists(config.DICT_CSV_PATH):
    print("Loading existing raw dictionary...")
    oecd_vocabulary = spark.read.option("header", True).csv(config.DICT_CSV_PATH) \
        .withColumn("relative_share", F.col("relative_share").cast("double")).withColumn("avg_sim", F.col("avg_sim").cast("double")) \
        .withColumn("global_count", F.col("global_count").cast("long")).cache()
else:
    print("Computing Dictionary from scratch...")
    packed_chunks = None
    for year in config.YEARS:
        try:
            df_year = spark.read.parquet(config.get_nlp_path(year))
            packed_chunks = df_year if packed_chunks is None else packed_chunks.unionByName(df_year, allowMissingColumns=True)
        except Exception as e: pass

    if packed_chunks is None:
        raise ValueError("CRITICAL ERROR: No data was loaded for ANY year. Check Phase 1 output.")

    valid_chunks = packed_chunks.select("doc_JobID", "doc_BGTOcc", F.explode(F.arrays_zip("noun_chunks", "sim_scores")).alias("zipped")) \
        .select("doc_JobID", "doc_BGTOcc", F.regexp_extract("doc_BGTOcc", r"(\d{4})", 1).alias("soc4"), F.col("zipped.noun_chunks").alias("noun_chunk"), F.col("zipped.sim_scores").alias("sim_data")) \
        .filter((F.col("sim_data") >= config.SIM_GROUNDING) & (F.col("soc4") != "")).cache()

    valid_job_universe = valid_chunks.select("doc_JobID").distinct().count()
    valid_jobs_anchor = valid_chunks.filter(F.col("soc4").isin(config.ALL_ANCHOR_SOCS)).select("doc_JobID").distinct().count()

    global_freq = valid_chunks.groupBy("noun_chunk").agg(F.countDistinct("doc_JobID").alias("global_count"), F.avg("sim_data").alias("avg_sim")).withColumn("share_economy", F.col("global_count") / (valid_job_universe if valid_job_universe > 0 else 1))
    anchor_freq = valid_chunks.filter(F.col("soc4").isin(config.ALL_ANCHOR_SOCS)).groupBy("noun_chunk").agg(F.countDistinct("doc_JobID").alias("count_anchor")).withColumn("share_anchor", F.col("count_anchor") / (valid_jobs_anchor if valid_jobs_anchor > 0 else 1))

    dictionary_df = global_freq.join(anchor_freq, "noun_chunk", "left").fillna(0).withColumn("max_relative_share", F.col("share_anchor") / F.col("share_economy"))
    
    # Using '5' instead of '500' for local synthetic data testing
    oecd_vocabulary = dictionary_df.filter((F.col("max_relative_share") >= config.REL_SHARE_THRESHOLD) & (F.col("global_count") >= 5) & (F.col("avg_sim") >= config.SIM_GROUNDING)).select("noun_chunk", F.col("max_relative_share").alias("relative_share"), "avg_sim", "global_count").cache()
    oecd_vocabulary.toPandas().to_csv(config.DICT_CSV_PATH, index=False)
    valid_chunks.unpersist()

# ==========================================
# PHASE 2.5: ENSEMBLE NLP POLISH (SUPERCHARGED + TQDM)
# ==========================================
print("\n--- PHASE 2.5: Ensemble NLP Polish ---")
if config.USE_EXISTING_POLISHED_DICTIONARY and os.path.exists(config.POLISHED_DICT_CSV_PATH):
    print("Loading existing polished dictionary...")
    oecd_vocabulary = spark.read.option("header", True).csv(config.POLISHED_DICT_CSV_PATH) \
        .withColumn("relative_share", F.col("relative_share").cast("double")).withColumn("avg_sim", F.col("avg_sim").cast("double")).withColumn("global_count", F.col("global_count").cast("long")).withColumn("gold_sim_score", F.col("gold_sim_score").cast("double")).cache()
else:
    print("Polishing dictionary against Gold Standard...")
    pdf = oecd_vocabulary.toPandas()
    
    if pdf.empty:
        print("  [WARNING] Raw dictionary is empty. Check Phase 2 thresholds.")
    else:
        try: nlp_driver = spacy.load("en_core_web_lg")
        except: nlp_driver = spacy.load("en_core_web_sm")
        
        from tqdm import tqdm  # Import the progress bar
        
        gold_docs = [nlp_driver(g) for g in config.GOLD_STANDARD]
        
        # 1. Extract phrases to a raw Python list for high-speed batching
        phrases = pdf['noun_chunk'].fillna("").astype(str).tolist()
        scores = []
        
        total_phrases = len(phrases)
        print(f"  -> Ready to process {total_phrases:,} dictionary terms...")
        
        # 2. Wrap nlp.pipe with tqdm for a real-time progress bar
        for doc in tqdm(nlp_driver.pipe(phrases, batch_size=2000), total=total_phrases, desc="Scoring against Gold Standard"):
            # Check if word is garbage/empty to avoid warnings
            if doc.has_vector and doc.vector_norm != 0:
                scores.append(float(max([doc.similarity(g) for g in gold_docs])) if gold_docs else 0.0)
            else:
                scores.append(0.0)

        # 3. Apply scores and filter
        pdf['gold_sim_score'] = scores
        polished_pdf = pdf[pdf['gold_sim_score'] >= 0.45]
        
        print(f"  -> Polishing Complete. Retained {len(polished_pdf):,} highly relevant terms.")
        polished_pdf.to_csv(config.POLISHED_DICT_CSV_PATH, index=False)

        schema = StructType([
            StructField("noun_chunk", StringType(), True), StructField("relative_share", DoubleType(), True),
            StructField("avg_sim", DoubleType(), True), StructField("global_count", LongType(), True), StructField("gold_sim_score", DoubleType(), True)
        ])
        oecd_vocabulary = spark.createDataFrame(polished_pdf[['noun_chunk', 'relative_share', 'avg_sim', 'global_count', 'gold_sim_score']].values.tolist(), schema=schema).cache()

        
# ==========================================
# PHASE 3: CLASSIFICATION & EXPORT (DYNAMIC)
# ==========================================
print("\n--- PHASE 3: Job Classification ---")
def run_classification_for_year(year):
    try: packed_chunks = spark.read.parquet(config.get_nlp_path(year))
    except: return None

    total_yearly_jobs = packed_chunks.count()
    print(f"  -> {year} Total Jobs in Economy: {total_yearly_jobs:,}")

    year_chunks = packed_chunks.select("doc_JobID", "doc_BGTOcc", F.explode(F.arrays_zip("noun_chunks", "sim_scores")).alias("zipped")).select("doc_JobID", "doc_BGTOcc", F.col("zipped.noun_chunks").alias("noun_chunk"), F.col("zipped.sim_scores").alias("sim_data")).filter(F.col("sim_data") >= config.SIM_GROUNDING)
    tagged_chunks = year_chunks.join(F.broadcast(oecd_vocabulary), "noun_chunk", "inner")
    job_scores = tagged_chunks.groupBy("doc_JobID", "doc_BGTOcc").agg(F.countDistinct("noun_chunk").alias("unique_data_terms")).withColumnRenamed("doc_BGTOcc", "soc_2020")
    job_scores = job_scores.withColumn("soc4", F.regexp_extract("soc_2020", r"(\d{4})", 1)).filter(F.col("soc4") != "")
    
    is_intensive = (F.col("unique_data_terms") >= config.DATA_THRESHOLD)
    classified = job_scores.withColumn("is_anchor", (F.col("soc4").isin(config.ALL_ANCHOR_SOCS) & is_intensive).cast("int")).withColumn("any_data_intensive", is_intensive.cast("int"))

    all_jobs_year = packed_chunks.select("doc_JobID", F.col("doc_BGTOcc").alias("soc_2020")).distinct().withColumn("soc4", F.regexp_extract("soc_2020", r"(\d{4})", 1)).filter(F.col("soc4") != "")
    merged = all_jobs_year.join(classified, ["doc_JobID", "soc4", "soc_2020"], "left").fillna(0)

    occ_sum = merged.groupBy("soc4").agg(F.count("*").alias("total_jobs"), F.sum("is_anchor").alias("anchor_jobs"), F.sum("any_data_intensive").alias("any_data_intensive_jobs")).withColumn("year", F.lit(year))
    return occ_sum.withColumn("total_data_share", 100 * F.col("any_data_intensive_jobs") / F.col("total_jobs")), job_scores.select(F.lit(year).alias("year"), F.col("doc_JobID"), F.col("soc4"), is_intensive.cast("int").alias("is_data_intensive_job")).filter((F.col("soc4") != "") & (is_intensive.cast("int") == 1))

occupation_summaries = {}
all_years_jobs_df = None
for y in config.YEARS:
    result = run_classification_for_year(y)
    if result:
        occupation_summaries[y] = result[0]
        all_years_jobs_df = result[1] if all_years_jobs_df is None else all_years_jobs_df.unionByName(result[1])

print("\n--- PHASE 3.5: Exporting Data-Intensive Job IDs ---")
if all_years_jobs_df is not None:
    all_years_jobs_df.toPandas().to_csv(config.JOB_EXPORT_FILE, index=False)
    print(f"✓ Saved to {config.JOB_EXPORT_FILE}")

# ==========================================
# PHASE 4: SECTOR MAPPING (DYNAMIC)
# ==========================================
print("\n--- PHASE 4: Applying Census Weights ---")
df_census = spark.read.option("header", True).csv(config.CENSUS_CSV)
desc_col = df_census.columns[0]
df_census = df_census.withColumn("soc4", F.regexp_extract(F.col(desc_col), r"^(\d{4})", 1))
sic_cols = [c for c in df_census.columns if c != desc_col and c != "soc4"]
stack_expr = f"stack({len(sic_cols)}, " + ", ".join([f"'{c}', `{c}`" for c in sic_cols]) + ") as (sic_col, count_raw)"
long_df = df_census.select("soc4", F.expr(stack_expr)).withColumn("sic2", F.regexp_extract("sic_col", r"^(\d{2})", 1).cast("int")).withColumn("n", F.regexp_replace("count_raw", ",", "").cast("long")).filter(F.col("n") > 0)
long_df = long_df.withColumn("SIC_Code", F.expr("CASE WHEN sic2 BETWEEN 1 AND 3 THEN 'A' WHEN sic2 BETWEEN 5 AND 39 THEN 'B-E' WHEN sic2 BETWEEN 41 AND 43 THEN 'F' WHEN sic2 BETWEEN 45 AND 56 THEN 'G-I' WHEN sic2 BETWEEN 58 AND 63 THEN 'J' WHEN sic2 BETWEEN 64 AND 66 THEN 'K' WHEN sic2 = 68 THEN 'L' WHEN sic2 BETWEEN 69 AND 82 THEN 'M-N' WHEN sic2 BETWEEN 84 AND 88 THEN 'O-Q' WHEN sic2 BETWEEN 90 AND 98 THEN 'R-T' WHEN sic2 = 99 THEN 'U' ELSE NULL END")).filter(F.col("SIC_Code").isNotNull())
totals = long_df.groupBy("soc4").agg(F.sum("n").alias("total_soc"))
weights_df = long_df.groupBy("soc4", "SIC_Code").agg(F.sum("n").alias("n_sic")).join(totals, "soc4").withColumn("w_soc4_SIC", F.col("n_sic") / F.col("total_soc")).cache()

sector_summaries = []
for year, occ_df in occupation_summaries.items():
    joined = occ_df.join(weights_df, "soc4", "left").fillna(0, subset=["w_soc4_SIC"])
    weighted = joined.select("SIC_Code", (F.col("total_jobs") * F.col("w_soc4_SIC")).alias("w_total"), (F.col("any_data_intensive_jobs") * F.col("w_soc4_SIC")).alias("w_any"))
    sec_sum = weighted.groupBy("SIC_Code").agg(F.sum("w_total").alias("total_jobs"), F.sum("w_any").alias("any_data_intensive_jobs"))
    sec_sum = sec_sum.withColumn("total_data_share", F.when(F.col("total_jobs") > 0, 100 * F.col("any_data_intensive_jobs") / F.col("total_jobs")).otherwise(0.0)).withColumn("year", F.lit(year))
    sector_summaries.append(sec_sum)

# ==========================================
# PHASE 5: ECONOMIC VALUATION
# ==========================================
print("\n--- PHASE 5: Calculating Economic Valuation ---")
full_sector_df = sector_summaries[0]
for d in sector_summaries[1:]: full_sector_df = full_sector_df.unionByName(d)

sut_df = spark.read.option("header", True).csv(config.SUT_CSV).filter(F.col("year") == config.SUT_YEAR).select(F.upper(F.trim("SIC_Code")).alias("SIC_Code"), (F.col("GVA_basic_prices").cast("double") * 1000000).alias("GVA_basic_prices"), (F.col("COMP_EMP").cast("double") * 1000000).alias("COMP_EMP"))
alpha_expr = F.create_map([F.lit(x) for i in config.ALPHA_MAP.items() for x in i])
valued = full_sector_df.withColumn("SIC_Code", F.upper(F.trim("SIC_Code"))).join(sut_df, "SIC_Code", "inner").withColumn("alpha_low", F.lit(config.ALPHA_LOW)).withColumn("alpha_avg", F.lit(config.ALPHA_ECONOMY_AVG)).withColumn("alpha_sector", F.coalesce(alpha_expr[F.col("SIC_Code")], F.lit(config.ALPHA_ECONOMY_AVG)))

valued = valued.withColumn("total_investment_sector", F.col("alpha_sector") * F.col("COMP_EMP") * (F.col("total_data_share")/100)) \
               .withColumn("inv_avg_tot", F.col("alpha_avg") * F.col("COMP_EMP") * (F.col("total_data_share")/100)) \
               .withColumn("inv_low_tot", F.col("alpha_low") * F.col("COMP_EMP") * (F.col("total_data_share")/100)) \
               .withColumn("inv_raw_wages", F.lit(1.0) * F.col("COMP_EMP") * (F.col("total_data_share")/100))

valued = valued.withColumn("inv_share_gva_sector", F.when(F.col("GVA_basic_prices")>0, (F.col("total_investment_sector")/F.col("GVA_basic_prices"))*100).otherwise(0.0)) \
               .withColumn("inv_share_gva_avg", F.when(F.col("GVA_basic_prices")>0, (F.col("inv_avg_tot")/F.col("GVA_basic_prices"))*100).otherwise(0.0)) \
               .withColumn("inv_share_gva_low", F.when(F.col("GVA_basic_prices")>0, (F.col("inv_low_tot")/F.col("GVA_basic_prices"))*100).otherwise(0.0)) \
               .withColumn("inv_share_gva_raw", F.when(F.col("GVA_basic_prices")>0, (F.col("inv_raw_wages")/F.col("GVA_basic_prices"))*100).otherwise(0.0)).cache()

# ==========================================
# PHASE 6: MASTER OFFLINE DASHBOARD
# ==========================================
print("\n--- PHASE 6: Generating Master Offline HTML Dashboard ---")
pdf = valued.toPandas()
pdf['year_str'] = pdf['year'].astype(str)

# Dynamically extract the domain name from config
target_domain_raw = "_and_".join(config.SOC_GROUPS.keys())
domain_name = target_domain_raw.replace("_", " ").title()

vocab_pdf = oecd_vocabulary.toPandas() if 'oecd_vocabulary' in locals() else pd.DataFrame()
polished_dict_count = len(vocab_pdf) if not vocab_pdf.empty else 0

try: raw_dict_count = len(pd.read_csv(config.DICT_CSV_PATH))
except Exception: raw_dict_count = "N/A (File missing)"

# Base HTML Structure
master_html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>OECD Macroeconomic Dashboard: {domain_name}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f6f9; color: #333; margin: 0; padding: 30px; }}
        .dashboard-header {{ text-align: center; padding: 25px; background: #002244; color: white; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
        .summary-card {{ background: #e9f2fb; padding: 25px; margin-bottom: 30px; border-radius: 8px; border-left: 5px solid #0055a4; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
        .summary-card ul {{ list-style-type: none; padding: 0; margin: 0; }}
        .summary-card li {{ margin-bottom: 10px; font-size: 15px; }}
        .summary-card strong {{ color: #002244; display: inline-block; width: 220px; }}
        .gold-tag {{ background: #ffd700; color: #856404; padding: 4px 10px; border-radius: 12px; font-size: 12px; font-weight: bold; margin-right: 5px; display: inline-block; margin-bottom: 5px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        .soc-tag {{ background: #002244; color: white; padding: 4px 10px; border-radius: 12px; font-size: 12px; font-weight: bold; margin-right: 5px; display: inline-block; margin-bottom: 5px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        .section-card {{ background: white; padding: 25px; margin-bottom: 30px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
        h2 {{ border-bottom: 2px solid #eaeaea; padding-bottom: 10px; color: #002244; margin-top: 0; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 15px; font-size: 13px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #002244; color: white; position: sticky; top: 0; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        .table-wrapper {{ max-height: 350px; overflow-y: auto; overflow-x: auto; margin-top: 10px; border: 1px solid #ddd; border-radius: 4px; }}
        .controls {{ display: flex; justify-content: space-between; align-items: center; margin-top: 15px; padding: 12px; background: #f0f4f8; border-radius: 6px; border: 1px solid #d9e2ec; }}
        .download-btn {{ background-color: #2ca02c; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; font-weight: bold; transition: background 0.2s; }}
        .download-btn:hover {{ background-color: #238023; }}
        details {{ width: 100%; }}
        summary {{ cursor: pointer; color: #0055a4; font-weight: bold; font-size: 15px; outline: none; }}
        summary:hover {{ color: #003366; }}
    </style>
    <script>
        function downloadCSV(tableId, filename) {{
            var table = document.getElementById(tableId);
            var rows = table.querySelectorAll("tr");
            var csv = [];
            for (var i = 0; i < rows.length; i++) {{
                var row = [], cols = rows[i].querySelectorAll("td, th");
                for (var j = 0; j < cols.length; j++) {{
                    // FIX: Using textContent to properly capture hidden table data
                    var data = cols[j].textContent.replace(/(\\r\\n|\\n|\\r)/gm, '').replace(/(\\s\\s)/gm, ' ').trim();
                    data = data.replace(/"/g, '""');
                    row.push('"' + data + '"');
                }}
                csv.push(row.join(","));
            }}
            var csvFile = new Blob([csv.join("\\n")], {{type: "text/csv"}});
            var downloadLink = document.createElement("a");
            downloadLink.download = filename + ".csv";
            downloadLink.href = window.URL.createObjectURL(csvFile);
            downloadLink.style.display = "none";
            document.body.appendChild(downloadLink);
            downloadLink.click();
            document.body.removeChild(downloadLink);
        }}
    </script>
</head>
<body>
    <div class="dashboard-header">
        <h1>OECD Macroeconomic Investment Dashboard</h1>
        <p>Sector: <strong>{domain_name}</strong> | Fully Offline Interactive Visualizations</p>
    </div>
    
    <div class="summary-card">
        <h2>Pipeline Execution Summary</h2>
        <ul>
            <li><strong>Anchor SOC Codes:</strong> {''.join([f'<span class="soc-tag">{soc}</span>' for soc in config.ALL_ANCHOR_SOCS])}</li>
            <li><strong>Gold Standard Baseline:</strong> {''.join([f'<span class="gold-tag">{word}</span>' for word in config.GOLD_STANDARD])}</li>
            <li><strong>Raw Statistical Dictionary:</strong> {raw_dict_count:,} unique terms discovered</li>
            <li><strong>Final Polished Dictionary:</strong> {polished_dict_count:,} terms survived semantic filtering</li>
        </ul>
    </div>
"""

section_counter = 0
plotly_engine_injected = False

def add_dashboard_section(title, content, data_df, is_plotly=True):
    global master_html, section_counter, plotly_engine_injected
    section_counter += 1
    table_id = f"table_{section_counter}"
    clean_filename = title.split(". ")[-1].replace(" ", "_").replace("%", "Pct")

    if is_plotly:
        if not plotly_engine_injected:
            visual_html = content.to_html(full_html=False, include_plotlyjs=True)
            plotly_engine_injected = True
        else:
            visual_html = content.to_html(full_html=False, include_plotlyjs=False)
    else:
        visual_html = content 

    table_html = data_df.to_html(index=False, table_id=table_id, float_format=lambda x: f"{x:,.2f}" if isinstance(x, float) else str(x))
    
    section = f"""
    <div class="section-card">
        <h2>{title}</h2>
        <div>{visual_html}</div>
        <div class="controls">
            <details>
                <summary>📊 Click to view/copy underlying chart data</summary>
                <div class="table-wrapper">
                    {table_html}
                </div>
            </details>
            <button class="download-btn" onclick="downloadCSV('{table_id}', '{clean_filename}')">📥 Download CSV</button>
        </div>
    </div>
    """
    master_html += section

# Base64 Word Cloud
if not vocab_pdf.empty:
    vocab_pdf['noun_chunk'] = vocab_pdf['noun_chunk'].astype(str).str.replace(r'\n|\r', ' ', regex=True)
    top_100_words = vocab_pdf.nlargest(100, 'relative_share')[['noun_chunk', 'relative_share', 'global_count']]
    word_freq = dict(zip(top_100_words['noun_chunk'], top_100_words['relative_share']))
    
    wc = WordCloud(width=1200, height=400, background_color='white', colormap='viridis', max_words=100)
    wc.generate_from_frequencies(word_freq)
    
    temp_wc_path = os.path.join(config.REPORTS_DIR, "temp_wc.png")
    wc.to_file(temp_wc_path)
    with open(temp_wc_path, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode()
    img_html = f'<div style="text-align:center;"><img src="data:image/png;base64,{encoded_string}" style="max-width:100%; border-radius:8px; box-shadow:0 0 10px rgba(0,0,0,0.1);"></div>'
    
    add_dashboard_section(f"1. {domain_name} Dictionary Word Cloud (Top 100)", img_html, top_100_words, is_plotly=False)
    os.remove(temp_wc_path) 
else:
    print("  [WARNING] Dictionary is empty. Skipping WordCloud generation.")

# Plotly Charts
econ = pdf.groupby("year_str")[["inv_raw_wages", "inv_low_tot", "inv_avg_tot", "total_investment_sector", "GVA_basic_prices"]].sum().reset_index()
econ_pct = econ.copy()
econ_pct["Raw Wages %"] = (econ_pct["inv_raw_wages"] / econ_pct["GVA_basic_prices"]) * 100
econ_pct["Conservative %"] = (econ_pct["inv_low_tot"] / econ_pct["GVA_basic_prices"]) * 100
econ_pct["Economy Avg %"] = (econ_pct["inv_avg_tot"] / econ_pct["GVA_basic_prices"]) * 100
econ_pct["Sector Map %"] = (econ_pct["total_investment_sector"] / econ_pct["GVA_basic_prices"]) * 100

fig_macro_abs = make_subplots(specs=[[{"secondary_y": True}]])
fig_macro_abs.add_trace(go.Scatter(x=econ['year_str'], y=econ['total_investment_sector'], name='Sector Map (Upper)', mode='lines+markers', line=dict(color='#00008b', width=4)), secondary_y=False)
fig_macro_abs.add_trace(go.Scatter(x=econ['year_str'], y=econ['inv_avg_tot'], name=f'Economy Avg (α={config.ALPHA_ECONOMY_AVG})', mode='lines+markers', line=dict(color='#2ca02c', width=4, dash='dash')), secondary_y=False)
fig_macro_abs.add_trace(go.Scatter(x=econ['year_str'], y=econ['inv_low_tot'], name=f'Conservative (α={config.ALPHA_LOW})', mode='lines+markers', line=dict(color='#a6cee3', width=4)), secondary_y=False)
fig_macro_abs.add_trace(go.Scatter(x=econ['year_str'], y=econ['inv_raw_wages'], name='Raw Wages', mode='lines+markers', line=dict(color='#ff7f0e', width=3, dash='dot')), secondary_y=False)
fig_macro_abs.add_trace(go.Scatter(x=econ['year_str'], y=econ['GVA_basic_prices'], name='Total GVA', mode='lines+markers', line=dict(color='#A9A9A9', width=3, dash='dot')), secondary_y=True)
fig_macro_abs.update_layout(title=f"Total UK Gross Value Added vs. {domain_name} Investment Scenarios", template="plotly_white")
add_dashboard_section(f"2. {domain_name} Absolute Macroeconomic Investment", fig_macro_abs, econ[['year_str', 'total_investment_sector', 'inv_avg_tot', 'inv_low_tot', 'inv_raw_wages', 'GVA_basic_prices']])

fig_macro_pct = px.line(econ_pct, x="year_str", y=["Sector Map %", "Economy Avg %", "Conservative %", "Raw Wages %"], title=f"{domain_name} Investment as a Percentage of Total UK Economy (GVA)", markers=True, template="plotly_white", color_discrete_sequence=["#00008b", "#2ca02c", "#a6cee3", "#ff7f0e"])
add_dashboard_section(f"3. {domain_name} Investment as % of GVA", fig_macro_pct, econ_pct[['year_str', 'Sector Map %', 'Economy Avg %', 'Conservative %', 'Raw Wages %']])

fig_sec_abs = px.bar(pdf, x="year_str", y=["GVA_basic_prices", "total_investment_sector"], facet_col="SIC_Code", facet_col_wrap=4, barmode="group", title=f"GVA vs. {domain_name} Investment by Sector (£)", template="plotly_white", color_discrete_map={"GVA_basic_prices": "#E5E5E5", "total_investment_sector": "#1f77b4"})
add_dashboard_section("4. Sector Absolute Breakdown", fig_sec_abs, pdf[['year_str', 'SIC_Code', 'total_investment_sector', 'GVA_basic_prices', 'total_jobs']])

fig_sec_pct = px.line(pdf, x="year_str", y="inv_share_gva_sector", color="SIC_Code", title=f"{domain_name} Investment Intensity by Sector (%)", markers=True, template="plotly_white")
add_dashboard_section("5. Sector Percentage Intensity", fig_sec_pct, pdf[['year_str', 'SIC_Code', 'inv_share_gva_sector', 'total_data_share']])

heatmap_data = pdf.pivot(index='SIC_Code', columns='year_str', values='total_data_share')
fig_heat = px.imshow(heatmap_data, title=f"Heatmap: Workforce in {domain_name} Roles (%)", color_continuous_scale="Blues", aspect="auto", template="plotly_white", text_auto=".1f")
add_dashboard_section("6. Heatmap: Workforce Intensity (%)", fig_heat, heatmap_data.reset_index())

heatmap_counts = pdf.pivot(index='SIC_Code', columns='year_str', values='any_data_intensive_jobs')
fig_heat_counts = px.imshow(heatmap_counts, title=f"Heatmap: Total Volume of {domain_name} Jobs", color_continuous_scale="Blues", aspect="auto", template="plotly_white", text_auto=",.0f")
add_dashboard_section("7. Heatmap: Absolute Job Volume", fig_heat_counts, heatmap_counts.reset_index())

alpha_df = pd.DataFrame([{"Sector": k, "Alpha": v, "Scenario": "Upper Bound"} for k, v in config.ALPHA_MAP.items()] + [{"Sector": k, "Alpha": config.ALPHA_LOW, "Scenario": "Lower Bound"} for k in config.ALPHA_MAP.keys()])
fig_alpha = px.bar(alpha_df, x="Sector", y="Alpha", color="Scenario", barmode="group", title="Alpha (α) Capital Markup Scenarios", template="plotly_white", color_discrete_map={"Upper Bound": "#1f77b4", "Lower Bound": "#a6cee3"})
fig_alpha.add_hline(y=config.ALPHA_ECONOMY_AVG, line_dash="dot", line_color="black")
add_dashboard_section("8. Valuation Engine: Alpha Scenarios", fig_alpha, alpha_df)

occ_frames = [df.toPandas() for y, df in occupation_summaries.items() if df is not None]
if occ_frames:
    all_occ_df = pd.concat(occ_frames, ignore_index=True)
    all_occ_df['soc4'] = all_occ_df['soc4'].astype(str)
    all_occ_df['year_str'] = all_occ_df['year'].astype(str)
    
    for plot_year in config.YEARS:
        year_data = all_occ_df[all_occ_df['year_str'] == str(plot_year)]
        
        top_50_pct = year_data.nlargest(50, "total_data_share").sort_values("total_data_share", ascending=True)
        fig_soc_pct = px.bar(top_50_pct, x="total_data_share", y="soc4", orientation='h', title=f"Top 50 {domain_name}-Intensive Occupations in {plot_year} (%)", template="plotly_white")
        fig_soc_pct.update_layout(height=800, yaxis=dict(type='category', dtick=1))
        add_dashboard_section(f"9. Top 50 Intensity Occupations ({plot_year})", fig_soc_pct, top_50_pct[['soc4', 'total_data_share', 'any_data_intensive_jobs', 'total_jobs']].sort_values("total_data_share", ascending=False))

        top_50_vol = year_data.nlargest(50, "any_data_intensive_jobs").sort_values("any_data_intensive_jobs", ascending=True)
        fig_soc_vol = px.bar(top_50_vol, x="any_data_intensive_jobs", y="soc4", orientation='h', title=f"Top 50 Occupations by Absolute {domain_name} Jobs in {plot_year}", template="plotly_white")
        fig_soc_vol.update_layout(height=800, yaxis=dict(type='category', dtick=1))
        add_dashboard_section(f"10. Top 50 Volume Occupations ({plot_year})", fig_soc_vol, top_50_vol[['soc4', 'any_data_intensive_jobs', 'total_data_share', 'total_jobs']].sort_values("any_data_intensive_jobs", ascending=False))

# Finalize and Save
master_html += "\n</body>\n</html>"
master_file_path = os.path.join(config.REPORTS_DIR, f"Master_Offline_Dashboard_{target_domain_raw}.html")

with open(master_file_path, "w", encoding="utf-8") as f:
    f.write(master_html)

print(f"✓ Successfully generated Master Offline Dashboard: {master_file_path}")

# ==========================================
# PHASE 7: AUDIT LOG EXPORT
# ==========================================
print("\n--- PHASE 7: Audit Log Generation ---")
with open(config.AUDIT_LOG_FILE, "w") as f:
    f.write(f"OECD PIPELINE FINAL AUDIT - {datetime.datetime.now()}\n")
    f.write(f"Total Jobs Tagged: {all_years_jobs_df.count() if all_years_jobs_df else 0}\n\n")
    f.write(valued.limit(100).toPandas().to_string())

print("\n" + "="*50)
print(f"PIPELINE COMPLETE. All reports saved to: {config.REPORTS_DIR}")
print("="*50 + "\n")