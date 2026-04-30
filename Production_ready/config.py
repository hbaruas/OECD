# ==========================================
# config.py - OECD PIPELINE CONFIGURATION
# ==========================================
import os

# --- 1. FILE PATHS & DIRECTORIES ---
BASE_PATH = "/Users/saurabhkumar/Desktop/OECD_PYSPARK_LOCAL/data/parquet_OECD"
PARQUET_SOURCE = os.path.join(BASE_PATH, "part-00000-6f2787d8-9f9c-4b9b-9903-fc9d83e3d0c0-c000.snappy.parquet")
CENSUS_CSV = "/Users/saurabhkumar/Desktop/OECD_PYSPARK_LOCAL/data/Census.csv"
SUT_CSV = "/Users/saurabhkumar/Desktop/OECD_PYSPARK_LOCAL/data/SUT_TABLE.csv"

# Perfectly anchors the path to wherever config.py is saved
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXPORT_DIR = os.path.join(BASE_DIR, "online_job_ads", "OECD")

REPORTS_DIR = os.path.join(EXPORT_DIR, "reports")

DICT_CSV_PATH = os.path.join(EXPORT_DIR, "oecd_dictionary_raw.csv")
POLISHED_DICT_CSV_PATH = os.path.join(EXPORT_DIR, "oecd_dictionary_polished.csv")
JOB_EXPORT_FILE = os.path.join(EXPORT_DIR, "job_level_export_data.csv")
AUDIT_LOG_FILE = os.path.join(EXPORT_DIR, "pipeline_audit_log_FINAL.txt")

def get_nlp_path(year):
    return os.path.join(BASE_PATH, "processed_data", str(year), "noun_chunks_packed")

# --- 2. PIPELINE TOGGLES ---
FORCE_RECOMPUTE_NLP = False                # Set True if running a completely new dataset
USE_EXISTING_DICTIONARY = False           # Set True to skip the dictionary polishing step and use the existing raw dictionary as-is.
USE_EXISTING_POLISHED_DICTIONARY = False   # Set True to skip the polishing step and use the existing polished dictionary. Only set this to True if you have already run the polishing step at least once and have a valid polished dictionary CSV in place. Otherwise, set to False to ensure the polishing step runs and generates a new polished dictionary.

# --- 3. MACRO PARAMETERS (Set for Local Synthetic Testing) ---
YEARS = [2020, 2021, 2022, 2023, 2024, 2025]
SAMPLE_FRACTION = 1.0  
REL_SHARE_THRESHOLD = 10.0  # Lowered for local testing (Default: 10.0)
SIM_GROUNDING = 0.35       # Lowered for local testing (Default: 0.35)
DATA_THRESHOLD = 3         

# --- 4. VALUATION & SUT PARAMETERS ---
SUT_YEAR = 2023
ALPHA_LOW = 1.58
ALPHA_ECONOMY_AVG = 3.62
ALPHA_MAP = {
    "A": 3.62, "B-E": 6.45, "F": 6.64, "G-I": 2.95, "J": 2.97,
    "K": 3.91, "L": 3.62, "M-N": 2.79, "O-Q": 2.07, "R-T": 3.06, "U": 3.62
}

# --- 5. SOC & ANCHOR CONFIGURATION (DYNAMIC) ---
# INSTRUCTIONS FOR ADDING NEW DOMAINS:
#
# STEP 1: Define your custom categories in SOC_GROUPS.
# - The dictionary key (e.g., "database" or "cyber_security") will automatically 
#   become the title of your HTML dashboard and chart labels. Use underscores for spaces.
# - The values must be a list of 4-digit UK SOC 2020 codes as strings.
# - You can run an isolated category, or stack multiple categories to measure the broader economy.
SOC_GROUPS = {
    "database": ["3133"],
    "analytics": ["3544", "2433"]
    # Example: "data_entry": ["4152"]
}

# STEP 2: The Master Anchor List
# - You MUST copy every single SOC code used in the SOC_GROUPS dictionary above and paste it here.
# - The PySpark cluster uses this master list to define the "target universe" for the 10.0x relative share math.
# - Example: If combining Database and Analytics above, this must be ["3133", "3544", "2433"]
ALL_ANCHOR_SOCS = ["3133"]

# STEP 3: The Semantic Bouncer (Gold Standard)
# - These are the undeniable, foundational core concepts for your target domain.
# - Every word discovered by the math must semantically match one of these terms to survive.
# - ISOLATED RUN: If running Database only, use strictly Database terms (sql, server, oracle).
# - COMBINED RUN: If combining Database + Analytics, you must merge both of their core 
#   terms into this single list so the AI allows both skill sets to pass the filter.
GOLD_STANDARD = [
    "database", "sql", "oracle", "relational database", "database administration", 
    "dbms", "data warehousing", "nosql", "database management", "server"
]