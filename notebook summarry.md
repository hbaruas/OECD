

Purpose of this notebook

This notebook reproduces (as closely as possible with ONS-available inputs) the OECD “Data as an Asset” job-advert methodology to measure data-intensive work in the labour market and then value it using Supply and Use Tables (SUT) concepts.

It does this in three stages:
	1.	Text-to-signal (NLP):
Extracts meaningful phrases from job adverts and measures how “data-related” those phrases are using semantic similarity.
	2.	Signal-to-occupation/industry indicators:
Aggregates those job-level signals into:
	•	Occupation-level “data intensity” (SOC 4-digit level)
	•	Industry-level “data intensity” (synthetic SIC Sections for demonstration / stakeholder replication)
	3.	Indicators-to-valuation (OECD-style):
Joins industry-level data intensity with SUT-derived industry totals (e.g., Compensation of Employees and GVA) to estimate a proxy value of data-related work and express it as % of GVA.
It also includes α (alpha) parameters to match OECD’s “capitalisation” logic (what fraction of data-related labour is treated as investment).

Why this is an “OECD-aligned adaptation” (and what differs)

OECD’s original approach relies on proprietary Lightcast/BGT occupational codes and industry metadata embedded in their pipeline. In ONS Textkernel job ads we do not have those codes, so this notebook adapts the approach by:
	•	Using SOC 2020 codes instead of BGTOcc/Lightcast taxonomy.
	•	Creating synthetic SIC Section labels from SOC for demonstration and for stakeholder replication of the full OECD valuation stage.
	•	Using ONS SUT-based industry totals (COMP_EMP and GVA at basic prices / purchasers’ prices depending on the chosen SUT table) rather than OECD’s cross-country industrial datasets.

Important: synthetic SIC is for demonstration and methodological walkthrough. For production-quality industry valuation, SIC should come from a real employer/establishment/industry mapping (or a defensible linking approach).

What this notebook outputs (key deliverables)

For each year processed (e.g., 2020–2025), the pipeline produces:

A) Job-level features (efficient storage)
	•	Per advert: number of noun chunks, number of data-related chunks, average similarity, and top-K most data-related chunks.

B) Job-level classification outputs
	•	Flags for data entry / database / data analytics categories (OECD-style buckets)
	•	“Any data-intensive” flag using a threshold rule

C) SOC 4-digit occupation summaries
	•	Data-intensive shares and category shares by SOC4 and year

D) Synthetic SIC sector summaries (for valuation)
	•	Data intensity shares by synthetic SIC section and year

E) OECD-style valuation metrics (Step 7–9)
	•	Industry “data investment” proxy and % of GVA
	•	Economy-wide time series 2020–2025
	•	α sensitivity scenarios (Low / Central / High)

How to run this notebook safely (restart logic)

This notebook is designed so that it can be restarted from later steps:
	•	All major outputs are written using a versioned safe-write pattern (writes to _v=timestamp and updates a _LATEST_POINTER).
	•	Any step can be re-run without deleting prior results.
	•	If a step already has outputs and FORCE_RECOMPUTE=False, it will skip work and use the latest output.

Local vs DAP environment
	•	Local development: set BASE_S3A_PATH to a local folder path (e.g., /Users/.../parquet_OECD) and confirm you can read/write parquet.
	•	DAP production: set BASE_S3A_PATH to the relevant s3a://.../OECD_DATA/ root and ensure Spark cluster resources are sufficient.

⸻

Cell-by-cell annotations (paste above each cell)

CELL 0 (Optional) — Build a local synthetic test dataset (commented-out cell)

What this cell is for
This cell generates a small synthetic parquet dataset that matches the expected schema used in the pipeline. It is intended for:
	•	testing logic locally before running at DAP scale
	•	testing downstream steps (classification, SIC mapping, valuation) without waiting for heavy NLP

What it does
	•	Reads a CSV (e.g., 1,000 job adverts sample)
	•	Enforces the target schema
	•	Creates synthetic SOC codes with balanced distribution across a pool
	•	Injects extra “data-heavy” keywords into selected SOCs to ensure the NLP stage produces a measurable signal
	•	Writes a single parquet folder output suitable for PARQUET_PATH

Key parameters to change
	•	INPUT_CSV_PATH: local sample CSV path
	•	OUTPUT_PARQUET_PATH: output folder for synthetic parquet
	•	SOC4_POOL: list of SOC4 codes for synthetic allocation
	•	data_keywords: injected keywords used to ensure the pipeline detects “data intensity”
	•	start_date, total_days: controls synthetic date range (2020–2025)

Outputs
	•	A local parquet folder containing synthetic job adverts with required columns and types.

When to use
	•	Use this when developing the notebook logic locally, validating outputs, or demonstrating the full pipeline on a small dataset.

⸻

CELL 1 — Spark session (reuse if already running)

Purpose
Starts Spark once and re-uses it if it is already running. This prevents accidental creation of multiple Spark sessions and supports “restart-from-cell-N” workflows.

What it does
	•	Checks whether spark already exists
	•	If yes: reuses the session and sets log level
	•	If no: creates a Spark session with memory/cores tuned for the pipeline

Parameters to consider
	•	spark.executor.memory, spark.driver.memory: controls memory available to tasks and driver
	•	spark.executor.cores, spark.driver.cores: controls parallelism and task scheduling
	•	spark.sql.adaptive.*: adaptive query execution and skew handling
	•	spark.serializer: Kryo improves performance for large processing

Operational guidance
	•	On DAP, update memory/cores to match the cluster profile you request.
	•	If you restart the notebook kernel, re-run this cell first.

⸻

CELL 2 — Global parameters (NO loose vars later)

Purpose
Defines all global parameters in one place so later cells do not depend on “hidden state” created earlier. This makes the pipeline reproducible and restartable.

What it typically contains
	•	Sampling controls (e.g., SAMPLE_FRACTION)
	•	NLP thresholds (e.g., SIM_THRESHOLD)
	•	Classification thresholds (e.g., DATA_THRESHOLD)
	•	Runtime flags (e.g., FORCE_RECOMPUTE, WRITE_DEBUG_CHUNKS)
	•	Debug sample settings (e.g., DEBUG_SAMPLE_FRACTION, TOP_K)

Why this matters
	•	Prevents runtime failures like NameError: SIM_THRESHOLD not defined
	•	Allows controlled experimentation (e.g., change one threshold and re-run only downstream cells)

Recommended defaults for development
	•	SAMPLE_FRACTION = 0.01 or 0.02 for fast iteration
	•	FORCE_RECOMPUTE = False so outputs are reused
	•	WRITE_DEBUG_CHUNKS = False unless troubleshooting

⸻

CELL 3 — Safe write utilities (versioned parquet writes for S3/local)

Purpose
Provides a safe and auditable output pattern for both local filesystems and object storage (S3A). This prevents accidental overwrites and supports reproducibility.

What it does
	•	Writes to a versioned path: .../<output_base>/_v=YYYYMMDD_HHMMSS
	•	Optionally verifies readability (small read)
	•	Writes a _SUCCESS_MARKER
	•	Updates a _LATEST_POINTER so downstream steps can always load the most recent output

Key functions
	•	safe_write_parquet_s3a(df, output_base_path, ...)
	•	read_latest_version(output_base_path)

Why this approach is used
	•	On S3/object storage, rename operations are not reliably atomic.
	•	Keeping older versions makes results auditable and recoverable.

⸻

CELL 4 — Paths + year selection

Purpose
Defines:
	•	The root folder for inputs and outputs
	•	Which years are processed
	•	The per-year output directories

What it does
	•	Parses YEARS (e.g., "ALL", "2020-2025", ["2020","2023"])
	•	Builds ALL_PATHS[year] for each output type
	•	Validates that paths are safely under BASE_S3A_PATH

Parameters
	•	BASE_S3A_PATH: local folder or s3a://.../OECD_DATA/
	•	YEARS: year range to process
	•	PARQUET_PATH: optional “single parquet source” mode
	•	CSV_PATH: optional override if CSVs are not under default structure

Outputs
	•	A dictionary of year-specific paths used consistently throughout the pipeline.

⸻

CELL 5 — Load job advert data (per year) with optional sampling

Purpose
Loads job adverts into all_data[year], applying light cleaning and an optional sampling fraction to support fast development runs.

What it reads
Either:
	•	CSV: .../csv_data/<year>/...
or
	•	Parquet: a unified parquet and then filters by year

Columns used downstream
	•	date
	•	job_id
	•	soc_2020
	•	job_title
	•	full_text

Sampling (SAMPLE_FRACTION)
If SAMPLE_FRACTION < 1.0, Spark uses:

df.sample(withReplacement=False, fraction=SAMPLE_FRACTION, seed=42)

This reduces runtime but maintains randomness.

Important runtime note
Calling .count() is an action and can be expensive. This cell counts to:
	•	confirm load success
	•	record the scale of processed data

For production, consider removing or minimising counts if needed.

Output
	•	all_data[yr] = DataFrame(...) for each year.

⸻

CELL 6 — NLP feature extraction (job-level, efficient storage)

Purpose
This is the computational core: it converts raw job advert text into job-level data intensity features without writing one row per noun chunk (efficiency improvement vs chunk-level storage).

What it extracts per job advert
	•	n_chunks_total: number of noun chunks in advert
	•	n_chunks_data: number of noun chunks above similarity threshold (SIM_THRESHOLD)
	•	avg_sim_data: average similarity among data-related chunks
	•	top_chunks: top-K most data-related chunks
	•	top_sims: similarity scores corresponding to top_chunks

Why noun chunks?
OECD uses noun chunks to capture “task-like phrases” (e.g., “SQL database”, “data pipeline”, “performance dashboard”), which are more meaningful than single keywords.

Why semantic similarity?
Instead of keyword matching, similarity uses word vectors to detect concept proximity to “data”.

Outputs
Writes per-year job_features under:
	•	processed_data/<year>/job_features/_v=...

Restart logic
If outputs exist and FORCE_RECOMPUTE=False, it will skip and use the latest version.

Operational note (why it can still be slow)
Even without writing chunk-level rows, the bottleneck remains:
	•	running spaCy over millions of documents
	•	noun chunk parsing and vector similarity computation
Sampling helps development; scaling requires more cores and distributed optimisation.

⸻

CELL 7 — Job classification + occupation summaries (SOC4)

Purpose
Implements the OECD-style category logic, using SOC as the available taxonomy.

Classification logic
	•	Create soc4 = first 4 digits of soc_2020
	•	Define SOC4 sets for:
	•	data_entry
	•	database
	•	data_analytics
	•	Define “data-intensive job” rule:
	•	n_chunks_data >= DATA_THRESHOLD
	•	Create job-level flags:
	•	data_entry, database, data_analytics
	•	any_data_intensive

Outputs
	1.	job_categories (job-level features + category flags)
	2.	occupation_summary (aggregated by SOC4):
	•	total jobs
	•	jobs per category
	•	category shares (%)
	•	total data-intensive share (%)

Digit level clarity (stakeholder question)
This notebook produces occupation estimates at SOC 4-digit level (soc4).
That is typically a defensible balance between:
	•	interpretability and stability (not too granular)
	•	meaningful differentiation across occupations

⸻

CELL 8 — Synthetic SIC mapping + sector summary (for OECD valuation replication)

Purpose
Creates a synthetic SIC Section from SOC to allow replication of the OECD industry valuation step when real industry codes are unavailable.

Important caveat
This is demonstration-only unless validated. It is a stakeholder-friendly bridge to replicate OECD’s final stage using SUT totals.

What it does
	•	Adds SICSection_synth from SOC major group (first digit)
	•	Aggregates to sector_summary_sic:
	•	total jobs
	•	category job counts
	•	category shares (%)
	•	total data-intensive share (%)

Outputs
	•	job_categories_sic (job-level with synthetic SIC)
	•	sector_summary_sic (industry-level summary)

⸻

CELL 9 — Load multi-year outputs for visualisation

Purpose
Loads the latest versions of:
	•	occupation summaries (SOC4)
	•	sector summaries (synthetic SIC)
into pandas for Plotly charts and reporting.

What it produces
	•	occupation_df (pandas)
	•	sector_df (pandas)

Note
This is a convenient analysis layer. The authoritative outputs remain the parquet files written earlier.

⸻

CELL 10 — Trend plots (occupation and economy-level)

Purpose
Creates time-series and faceted charts across 2020–2025. These plots support:
	•	narrative storytelling in presentations
	•	identifying shifts in data intensity over time

Typical plots
	•	Economy-wide share of data-intensive job adverts over time
	•	Top-N occupations each year by data intensity (faceted)
	•	Category composition stacked (entry/database/analytics)

Interpretation guidance
Charts show diffusion of data-related tasks beyond “traditional data jobs”.

⸻

CELL 11 — SUT join + OECD-style valuation (Steps 7–9)

Purpose
Replicates OECD’s valuation logic:
	•	combine industry-level data intensity with industry totals from SUT
	•	compute a proxy “data investment” and express it as % of GVA
	•	generate time series (2020–2025) and α sensitivity scenarios

Inputs
	•	sector_summary_sic for each year (synthetic SIC section and shares)
	•	SUT table (e.g., 2023 industry totals)
	•	GVA_basic_prices
	•	COMP_EMP

OECD α parameters
OECD uses α to represent what proportion of data-related labour is treated as investment:
	•	Data entry: lower α
	•	Database: medium α
	•	Analytics: higher α

This notebook supports:
	•	Central / Low / High scenarios for sensitivity bands.

Outputs (conceptual)
	•	Industry-level data_investment_share_of_GVA
	•	Economy-wide time series
	•	α sensitivity table (Low/Central/High)

Note about year coverage
Even if the SUT is only available for 2023 (most recent), it can be applied to all job-ad years as a demonstration (holding SUT totals constant).
For a production-grade series, you would use SUT totals by year.

⸻

One extra “future-proofing” block to add near the top (recommended)

Parameters quick reference (add as a markdown section near CELL 2)
	•	YEARS: which years to process (e.g., "2020-2025", "ALL")
	•	SAMPLE_FRACTION: fraction of job adverts to sample per year (e.g., 0.01)
	•	SIM_THRESHOLD: similarity threshold above which a noun chunk is treated as “data-related” (e.g., 0.45)
	•	DATA_THRESHOLD: minimum number of data-related chunks in a job advert to flag it as data-intensive (e.g., 3)
	•	TOP_K: number of top data-related noun chunks stored per job advert (e.g., 10)
	•	FORCE_RECOMPUTE: if True, ignores cached outputs and re-runs expensive stages
	•	WRITE_DEBUG_CHUNKS: if True, writes a small chunk-level debug dataset (useful for QA)

