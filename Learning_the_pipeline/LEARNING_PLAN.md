# Learning Plan: OECD PySpark Pipeline (11 Days × 1 Hour)

## What This Pipeline Actually Does

In plain English: it takes **62.3 million job adverts**, uses NLP to extract skill phrases from job descriptions, identifies which phrases are statistically over-represented in target occupations (e.g. database administrators, data analysts), labels each job advert as "data-intensive" or not, maps those jobs onto UK industry sectors using Census data, then multiplies the labour share by an economic multiplier (alpha) to estimate the total investment in data skills as a percentage of UK GDP (GVA).

---

## The 7 Phases at a Glance

| Phase | What it does | Key tech |
|---|---|---|
| Phase 0 | Pre-flight cleanup | Spark cache, file deletion |
| Phase 1 | NLP extraction — pull noun chunks from job text | spaCy `mapInPandas` |
| Phase 2 | Build OECD vocabulary — relative share math | Spark groupBy/agg |
| Phase 2.5 | Semantic polish — gold standard filter | spaCy word vectors |
| Phase 3 | Classify each job as data-intensive or not | Broadcast join |
| Phase 4 | Map SOC occupations → SIC sectors via Census weights | Spark `stack()` pivot |
| Phase 5 | Economic valuation: alpha × wages × share | SUT table join |
| Phase 6 | Generate offline HTML dashboard | Plotly + WordCloud |

---

## The 3 Input Files

1. **Parquet job ads** — 62M rows: `job_id`, `full_text`, `soc_2020`, `date`
2. **Census.csv** — SOC occupation × SIC industry counts (how many workers per occupation are in each sector)
3. **SUT_TABLE.csv** — UK Supply and Use Table: GVA and COMP_EMP per SIC sector and year

---

## The 3 Key Thresholds (Memorise These)

| Parameter | Value | Meaning |
|---|---|---|
| `SIM_GROUNDING` | 0.35 | Minimum cosine similarity to the word "data" for a noun chunk to be kept |
| `REL_SHARE_THRESHOLD` | 10.0 | A term must appear 10× more often in anchor jobs than the general economy |
| `DATA_THRESHOLD` | 3 | A job needs at least 3 unique dictionary terms to be classified as data-intensive |

---

## 11-Day Schedule

---

### Day 1: Big Picture & Config (no code)

**Goal:** Understand the full pipeline before touching a line of code.

1. Read `config.py` top to bottom and narrate every parameter aloud — what it controls, what happens if you change it
2. On paper, draw the data flow: 3 inputs → 7 phases → outputs (dictionary CSV, job export CSV, HTML dashboard)
3. Understand the 3 key thresholds above
4. Understand the toggle flags (`USE_EXISTING_DICTIONARY`, `FORCE_RECOMPUTE_NLP`) — these are the "skip recomputing" flags

**Exit check:** Can you explain to someone what the pipeline produces, what the inputs are, and what the 3 thresholds mean? If yes, done.

---

### Day 2: PySpark Fundamentals

**Goal:** Understand the PySpark patterns the pipeline relies on. No need to learn all of Spark — only what's in this code.

1. **SparkSession and config** (`main.py:36-41`)
   - `local[4]` = run on your laptop using 4 CPU cores
   - `spark.driver.memory` = RAM for the coordinator process
   - `spark.sql.shuffle.partitions = 10` = keep shuffle small on a laptop (default is 200)

2. **Core DataFrame operations used in this pipeline:**
   - `spark.read.parquet()` — reading columnar compressed files
   - `filter()`, `withColumn()`, `select()` — row/column operations
   - `groupBy().agg()` — aggregation
   - `join()` — joining two DataFrames on a key
   - `cache()` / `unpersist()` — pin a DataFrame in RAM so Spark doesn't recompute it

3. **Understand lazy evaluation:** Spark doesn't run anything until you call an action (`.count()`, `.toPandas()`, `.write`). All the `withColumn`/`filter` calls are just building a plan.

4. **Practice:** Write a 10-line PySpark script that reads a parquet file, filters by year, counts rows, and prints the count.

**Exit check:** You can explain what `cache()` does and why it matters, and why `.count()` is expensive.

---

### Day 3: The Data Source — Parquet & Schema

**Goal:** Understand what's in the data and why it's stored in Parquet.

1. **What is Parquet?**
   - Columnar format — reads only the columns you ask for, ignores the rest
   - Snappy compression — fast decompression, good compression ratio
   - Spark's native format — no parsing overhead like CSV
   - Why it matters at 62M rows: reading `full_text` (large) and `date` (small) separately avoids loading irrelevant columns

2. **The schema you need to memorise:**
   - `job_id` — unique identifier per advert
   - `full_text` — the raw job description text (this is what spaCy reads)
   - `soc_2020` — UK Standard Occupational Classification code (e.g. `"2433 - Data analysts"`)
   - `date` — the date the advert was posted

3. **Understand `partitionBy("doc_month")`** (`main.py:123`):
   - Phase 1 writes noun chunk output partitioned by month
   - This means Spark creates subfolders like `/year=2023/doc_month=1/`
   - Benefit: when you later filter by month, Spark only reads that subfolder

4. **Key pattern to memorise** (`main.py:117`):
   ```python
   df_raw = spark.read.parquet(source_path)
       .withColumn("date", F.to_date("date"))
       .filter(F.year("date") == year)
       .withColumn("doc_year", F.year("date"))
       .withColumn("doc_month", F.month("date"))
   ```

**Exit check:** Why is Parquet better than CSV for 62M rows? What are the 4 columns you need?

---

### Day 4: Phase 1 — NLP Extraction with `mapInPandas`

**Goal:** Understand how spaCy runs inside Spark.

1. **The problem:** Spark works on distributed data. spaCy is a single-machine Python library. `mapInPandas` is the bridge — it sends chunks of rows (as Pandas DataFrames) to spaCy, collects the results, and hands them back to Spark.

2. **Understand the function `extract_noun_chunks_packed`** (`main.py:76-104`):
   - Input: an iterator of Pandas DataFrames (Spark sends batches)
   - For each row: run spaCy on the `full_text`, extract noun chunks
   - For each noun chunk: clean it (lowercase, strip punctuation, 1-4 words only)
   - Compute cosine similarity between the chunk and the word `"data"`
   - Output: a row with `noun_chunks` (list) and `sim_scores` (list)

3. **spaCy noun chunks:** These are grammatically meaningful noun phrases extracted from text — e.g. "database management", "sql server", "data governance". They're more informative than individual words.

4. **Why `en_core_web_lg`?** The large model has 300-dimensional word vectors needed for similarity computation. The small model (`en_core_web_sm`) has no vectors.

5. **The output schema** (`main.py:70-74`):
   ```
   doc_JobID, doc_BGTOcc, doc_year, doc_month, noun_chunks (array), sim_scores (array)
   ```
   One row per job, with all its noun chunks packed into arrays.

**Exit check:** Draw the flow: job text → spaCy → noun chunks → similarity filter → arrays. What does `batch_size=50` do?

---

### Day 5: Phase 2 — Building the OECD Dictionary (the Core Idea)

**Goal:** Understand the relative share calculation — this is the intellectual heart of the pipeline.

1. **The concept:**
   - A term like "sql" appears in 80% of Database Administrator job ads but only 2% of all job ads
   - Relative share = 80% / 2% = 40x — this term is 40 times more concentrated in target jobs
   - The pipeline keeps any term with relative share ≥ 10x
   - This is essentially a labour-market TF-IDF

2. **The two universe sizes** (`main.py:151-152`):
   - `valid_job_universe` = total distinct jobs in the whole economy (denominator for `share_economy`)
   - `valid_jobs_anchor` = distinct jobs in anchor SOC codes only (denominator for `share_anchor`)

3. **Key Spark patterns to memorise** (`main.py:154-155`):
   ```python
   # Count how many distinct jobs contain each noun chunk
   global_freq = valid_chunks.groupBy("noun_chunk").agg(
       F.countDistinct("doc_JobID").alias("global_count"),
       F.avg("sim_data").alias("avg_sim")
   )
   anchor_freq = valid_chunks.filter(F.col("soc4").isin(config.ALL_ANCHOR_SOCS))
       .groupBy("noun_chunk").agg(F.countDistinct("doc_JobID").alias("count_anchor"))
   ```

4. **`explode(arrays_zip(...))`** (`main.py:147`): Converts one row with two arrays (`[chunk1, chunk2]`, `[sim1, sim2]`) into two rows — one per chunk. This is how you "unpack" the Phase 1 output.

5. **The triple filter** (`main.py:160`): `relative_share >= 10` AND `global_count >= 500` AND `avg_sim >= 0.35`

**Exit check:** If "spreadsheet" appears in 1% of all jobs and 15% of anchor jobs, what is its relative share? Does it pass the threshold? *(Answer: 15x — yes it passes 10x threshold, but may fail gold standard in Phase 2.5)*

---

### Day 6: Phase 2.5 — Gold Standard Semantic Polish

**Goal:** Understand why a second NLP filter is added on top of the statistical dictionary.

1. **The problem with pure statistics:** The relative share math might surface terms like "spreadsheet" in a "database" run because database admins also use spreadsheets. The gold standard filter removes these statistically-overrepresented but semantically-irrelevant terms.

2. **How it works:**
   - Take every term that survived Phase 2 (e.g. 5,000 terms)
   - For each term, compute its cosine similarity to each word in `GOLD_STANDARD` (e.g. "database", "sql", "oracle")
   - Keep the maximum similarity across all gold words
   - Keep terms where `max_similarity >= 0.45`

3. **Why run on the driver (not Spark)?** The dictionary is small after Phase 2 filtering. Converting to Pandas (`.toPandas()`) and using `nlp.pipe()` with `batch_size=2000` is fast enough. No need for distributed computing.

4. **tqdm** (`main.py:194`): Shows a progress bar. At 5,000+ terms this can take minutes.

5. **The dual-stage design:** Phase 2 = statistical filter (cast a wide net), Phase 2.5 = semantic filter (remove noise). Both are necessary.

**Exit check:** If a term has `relative_share = 15` but `gold_sim_score = 0.3`, does it survive? Why? *(Answer: No — it fails the 0.45 gold standard threshold)*

---

### Day 7: Phase 3 — Job Classification

**Goal:** Understand how each of 62M jobs gets a label.

1. **Broadcast join** (`main.py:227`):
   ```python
   tagged_chunks = year_chunks.join(F.broadcast(oecd_vocabulary), "noun_chunk", "inner")
   ```
   - The vocabulary is small (~hundreds of terms) — it fits in each executor's memory
   - `F.broadcast()` tells Spark: don't shuffle the big DataFrame, just send the small one everywhere
   - Without broadcast, Spark would do an expensive shuffle of the 62M-row dataset

2. **The classification rule** (`main.py:231-232`):
   ```python
   is_intensive = (F.col("unique_data_terms") >= config.DATA_THRESHOLD)  # >= 3
   ```
   A job is "data-intensive" if it contains at least 3 distinct vocabulary terms.

3. **The left join with all jobs** (`main.py:234-235`):
   - After classifying jobs that matched vocabulary terms, rejoin with ALL jobs
   - Jobs with 0 matches get `is_anchor=0` and `any_data_intensive=0` from `fillna(0)`
   - This ensures every job in the universe is counted in the denominator

4. **Two levels of classification:**
   - `is_anchor` — SOC code is in the target list AND data-intensive (strict)
   - `any_data_intensive` — data-intensive regardless of SOC code (broad)

5. **The year loop** (`main.py:242-246`): Classification is run year-by-year and results are accumulated into `occupation_summaries` (a dict of DataFrames by year).

**Exit check:** Why does the pipeline do a left join after the vocabulary join? What would happen if it didn't? *(Answer: Jobs with zero matches would be dropped, making the denominator wrong — share % would be inflated)*

---

### Day 8: Phase 4 — Census Sector Mapping (SOC → SIC)

**Goal:** Understand how occupation-level data becomes sector-level data.

1. **The problem:** Phase 3 gives you `total_data_share` per SOC occupation code. But economists think in sectors (agriculture, manufacturing, finance). You need to distribute each occupation across the sectors where those workers actually work.

2. **The Census matrix:** The Census CSV has rows = occupations (SOC4 codes), columns = industry sectors (SIC codes), values = number of workers. E.g. row "2433" (data analysts), column "58-63" (information sector): 45,000 workers.

3. **The `stack()` pivot** (`main.py:261`):
   ```python
   stack_expr = f"stack({n}, 'col1', col1, 'col2', col2, ...) as (sic_col, count_raw)"
   ```
   This converts wide format (one row, many SIC columns) to long format (one row per SOC-SIC pair). This is Spark's way of doing `pd.melt()`.

4. **Weight calculation** (`main.py:265`):
   ```python
   w_soc4_SIC = n_sic / total_soc  # What fraction of SOC X workers are in sector Y?
   ```
   E.g. 30% of data analysts work in sector J (information), 20% in sector K (finance), etc.

5. **Applying weights** (`main.py:269-271`): Multiply job counts by weights to distribute across sectors.

6. **SIC sector codes used:**

   | SIC Code | Sector |
   |---|---|
   | A | Agriculture |
   | B-E | Mining, Manufacturing, Utilities |
   | F | Construction |
   | G-I | Wholesale, Retail, Transport, Hotels |
   | J | Information & Communication |
   | K | Financial & Insurance |
   | L | Real Estate |
   | M-N | Professional & Business Services |
   | O-Q | Public Admin, Education, Health |
   | R-T | Arts, Entertainment, Other Services |
   | U | Extraterritorial |

**Exit check:** If SOC 2433 has 100 total jobs, 60 data-intensive jobs, and 40% of these workers are in sector J — how many sector-J data-intensive jobs are attributed to SOC 2433? *(Answer: 60 × 0.40 = 24 jobs)*

---

### Day 9: Phase 5 — Economic Valuation

**Goal:** Understand the alpha multiplier formula and why it gives a GVA-comparable investment figure.

1. **The SUT Table:** Contains two numbers per sector per year:
   - `GVA_basic_prices` — total output of that sector (in £)
   - `COMP_EMP` — total wages and salaries paid in that sector (in £)

2. **The core investment formula** (`main.py:286-289`):
   ```python
   total_investment = alpha × COMP_EMP × (data_share / 100)
   ```
   In plain English: "Of all the wages in this sector, `data_share%` went to data-intensive workers. Multiply by alpha to get the total economic value of that investment."

3. **Why alpha?** Wages are just one component of economic value. Capital, software, training, and infrastructure multiply the impact. Alpha captures this multiplier:

   | Alpha value | Scenario |
   |---|---|
   | 1.0 | Raw wages (lower bound, no multiplier) |
   | 1.58 | Conservative estimate |
   | 3.62 | Economy average (default) |
   | 6.64 | Construction sector (higher capital intensity) |

4. **Investment as % of GVA** (`main.py:291-294`):
   ```python
   inv_share_gva = (total_investment / GVA_basic_prices) × 100
   ```
   This makes the number comparable to standard national accounts statistics.

5. **Three scenarios** run simultaneously: sector-specific alpha, economy average alpha, conservative alpha — giving upper/lower bounds.

**Exit check:** Sector J has COMP_EMP of £100bn and 5% data share. With alpha=3.62, what is total investment? What if GVA is £150bn — what is the investment share? *(Answer: £100bn × 0.05 × 3.62 = £18.1bn. Share = 18.1/150 = 12.1% of GVA)*

---

### Day 10: Phase 6 — Dashboard Generation

**Goal:** Understand how the HTML dashboard is assembled.

1. **The `add_dashboard_section` pattern** (`main.py:389-421`):
   - Each section = one Plotly chart + one collapsible HTML table + one CSV download button
   - The first chart call includes the full Plotly.js library (`include_plotlyjs=True`)
   - All subsequent calls omit it (`include_plotlyjs=False`) to keep file size down
   - The HTML is built by concatenating strings into `master_html`

2. **WordCloud** (`main.py:428-438`):
   - Takes the top 100 dictionary terms weighted by `relative_share`
   - Renders to a PNG file, encodes it as base64, embeds it in HTML as a `data:` URI
   - This makes the HTML truly self-contained (no external image files needed)

3. **Plotly chart types used:**
   - `px.line` — time series (investment over years)
   - `px.bar` — sector breakdowns and top occupation rankings
   - `px.imshow` — heatmaps (workforce intensity by sector × year)
   - `make_subplots` — dual-axis chart (investment vs GVA on different scales)

4. **The occupation charts loop** (`main.py:488-499`): For each year, creates two charts:
   - Top 50 by intensity (% of jobs that are data-intensive)
   - Top 50 by volume (absolute number of data-intensive jobs)

5. **Phase 7 — Audit log** (`main.py:514-517`): Writes a plain text file with the timestamp, total jobs tagged, and the first 100 rows of the valued DataFrame for human review.

**Exit check:** Why does the dashboard use base64 for the WordCloud image? What does `full_html=False` do in Plotly? *(Answer: base64 makes the HTML self-contained with no external file dependencies. `full_html=False` returns just the chart div, not a complete HTML document — so you can embed multiple charts in one page)*

---

### Day 11: Rebuild from Scratch (Memory Test)

**Goal:** Code the entire pipeline from a blank file, no references, just the dataset and your brain.

**The order to rebuild in:**

1. `config.py` first (5 min):
   - Paths, toggles (`FORCE_RECOMPUTE_NLP`, `USE_EXISTING_DICTIONARY`)
   - Years, thresholds (`SIM_GROUNDING`, `REL_SHARE_THRESHOLD`, `DATA_THRESHOLD`)
   - `SOC_GROUPS`, `ALL_ANCHOR_SOCS`, `GOLD_STANDARD`
   - Alpha map, SUT year

2. `main.py` — Phase 0 (3 min): SparkSession, clear cache, delete stale files

3. `main.py` — Phase 1 (10 min): The `extract_noun_chunks_packed` function and the year loop that calls it with `mapInPandas`

4. `main.py` — Phase 2 (8 min): Load packed chunks, `explode(arrays_zip(...))`, compute `global_freq` and `anchor_freq`, join, filter on triple condition, save to CSV

5. `main.py` — Phase 2.5 (5 min): Load vocabulary, `nlp.pipe()` loop with tqdm, filter on `gold_sim_score >= 0.45`

6. `main.py` — Phase 3 (8 min): The `run_classification_for_year` function — broadcast join, threshold, left join with all jobs, year loop

7. `main.py` — Phase 4 (8 min): Read Census, stack pivot, weight calculation, apply weights per year

8. `main.py` — Phase 5 (5 min): Read SUT, join with sector data, apply alpha formula, compute GVA share

9. `main.py` — Phase 6 (8 min): The `add_dashboard_section` helper, WordCloud section, all Plotly charts, write HTML

10. `main.py` — Phase 7 (1 min): Audit log

**Scoring yourself:** If you get 7/10 phases coded correctly from memory without looking anything up, you understand this pipeline. The two hardest to recall are the `stack()` pivot (Phase 4) and the `arrays_zip` + `explode` pattern (Phase 2).

---

## The 5 Patterns That Appear Everywhere

Memorise these five and the rest falls into place:

| Pattern | Where | Why |
|---|---|---|
| `mapInPandas(fn, schema)` | Phase 1 | Run Python/spaCy inside Spark |
| `explode(arrays_zip(arr1, arr2))` | Phase 2, 3 | Unpack parallel arrays into rows |
| `groupBy().agg(countDistinct(...))` | Phase 2 | Count unique jobs per term |
| `join(broadcast(small_df), key, "inner")` | Phase 3 | Efficient join when one side is small |
| `stack(n, 'col', val, ...) as (name, val)` | Phase 4 | Wide-to-long pivot in Spark SQL |

---

## Risks (What Could Trip You Up)

- **HIGH:** `mapInPandas` — the schema must exactly match the returned DataFrame columns and types. One wrong type causes a cryptic Spark error.
- **HIGH:** spaCy model size — `en_core_web_lg` must be downloaded (`python -m spacy download en_core_web_lg`). The small model has no vectors.
- **MEDIUM:** The `stack()` expression is dynamically generated from the CSV column names — if the Census CSV column names change, this breaks.
- **MEDIUM:** `ALL_ANCHOR_SOCS` must be a strict subset of the codes in `SOC_GROUPS` values — if they drift, the relative share math is wrong.
- **LOW:** Memory — `cache()` on large DataFrames on a laptop can cause OOM. Know when to `unpersist()`.

---

## Quick Reference: File Locations

| File | Location |
|---|---|
| Config | `Production_ready/config.py` |
| Main pipeline | `Production_ready/main.py` |
| Raw dictionary output | `Production_ready/online_job_ads/OECD/oecd_dictionary_raw.csv` |
| Polished dictionary | `Production_ready/online_job_ads/OECD/oecd_dictionary_polished.csv` |
| Job-level export | `Production_ready/online_job_ads/OECD/job_level_export_data.csv` |
| HTML dashboards | `Production_ready/online_job_ads/OECD/reports/` |
| Audit log | `Production_ready/online_job_ads/OECD/pipeline_audit_log_FINAL.txt` |
| NLP parquet cache | `data/parquet_OECD/processed_data/{year}/noun_chunks_packed/` |
