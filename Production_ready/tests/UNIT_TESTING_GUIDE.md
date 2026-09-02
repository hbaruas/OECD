# Unit Testing Guide
## From Zero to Production-Ready — Everything You Need to Know

This document is written for someone who knows Python and knows this pipeline, but has never written a unit test before. By the end you will understand not just how to run the tests in this project, but why every decision was made — well enough to write new tests from scratch for any future pipeline you work on.

---

## 1. What is a unit test and why does it exist?

A unit test is a piece of code that calls one small piece of your main code and checks that it produced the right answer.

Here is the simplest possible example:

```python
def add(a, b):
    return a + b

def test_add():
    assert add(2, 3) == 5
```

That is a complete unit test. You call `add`, you assert the result is what you expected. If it is, the test passes (green). If it is not, the test fails (red) and tells you exactly what went wrong.

**Why bother?** Consider what happens without tests:

1. You change a line in Phase 2 of the pipeline to fix a bug
2. That change accidentally breaks Phase 3's left join logic
3. The pipeline runs to completion with no error
4. The dashboard looks plausible
5. You present the results to stakeholders
6. Three weeks later someone notices the investment numbers are wrong

With tests, step 2 is caught immediately — before you even save the file. The test for the left join runs in one second and turns red. You know exactly what broke and exactly where.

At 62 million rows, you cannot run the full pipeline every time you make a change. Tests let you verify the logic is correct in seconds, on a laptop, with synthetic data.

---

## 2. The difference between unit tests, integration tests, and end-to-end tests

These three terms describe how much of the system a test exercises:

| Type | Scope | Speed | When it catches issues |
|---|---|---|---|
| **Unit** | One function or formula | Milliseconds | The moment you write bad logic |
| **Integration** | Multiple components working together | Seconds–minutes | When components misunderstand each other |
| **End-to-end** | The entire pipeline on real data | Hours | When the whole system has an environmental issue |

This project uses **unit tests** and light **integration tests** (Spark-based tests that spin up a local cluster). There are no end-to-end tests — running the full pipeline on 62 million records is the end-to-end test, and you do that manually.

The Spark tests in this project sit between unit and integration: they test one phase of logic, but they need a real (local) Spark session to do it. That is an acceptable trade-off because Spark's DataFrame API is impossible to test meaningfully without actually running Spark.

---

## 3. Why pytest, and not something else?

Python has a built-in testing library called `unittest`. It works, but it requires a lot of ceremony:

```python
# unittest — verbose and boilerplate-heavy
import unittest

class TestAdd(unittest.TestCase):
    def test_add(self):
        self.assertEqual(add(2, 3), 5)

if __name__ == "__main__":
    unittest.main()
```

**pytest** does the same thing with far less code:

```python
# pytest — clean and simple
def test_add():
    assert add(2, 3) == 5
```

pytest also:
- Automatically finds all test files and functions (no registration needed)
- Produces readable error messages that show the actual vs expected values
- Supports `fixtures` (explained below) which `unittest` handles awkwardly
- Has a huge ecosystem of plugins
- Is the industry standard — every Python job you will encounter uses it

The alternative to pytest would be `nose2` or `unittest`, but there is no good reason to use either in a new project today.

---

## 4. How pytest finds your tests

pytest uses a discovery convention. When you run `pytest` in a directory it:

1. Looks for files named `test_*.py` or `*_test.py`
2. Inside those files, finds functions named `test_*`
3. Runs each one and reports pass/fail

This is why all files in the `tests/` folder start with `test_` and all functions inside them start with `test_`. If you name a function `check_something()` instead of `test_check_something()`, pytest will silently ignore it.

The `pyproject.toml` file tells pytest where to look:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_functions = ["test_*"]
addopts = "-v --tb=short"
```

- `testpaths = ["tests"]` — only look inside the `tests/` folder
- `addopts = "-v --tb=short"` — always run verbose mode with short tracebacks (you do not need to type `-v` every time)

---

## 5. The anatomy of a test

Every test in this project follows the same three-step pattern, called **AAA: Arrange, Act, Assert**.

```python
def test_investment_formula():
    # ARRANGE — set up your inputs
    alpha = 3.62
    comp_emp = 100_000_000_000
    data_share = 5.0

    # ACT — call the thing you are testing
    result = alpha * comp_emp * (data_share / 100)

    # ASSERT — check the output is what you expected
    assert abs(result - 18_100_000_000) < 1
```

**Why `abs(result - expected) < 1` instead of `result == 18_100_000_000`?**

Because floating point arithmetic on computers is not exact. `3.62 * 100_000_000_000 * 0.05` might return `18100000000.000002` due to binary rounding. Using `abs(difference) < 1` (or `< 1e-9` for higher precision) avoids false failures caused by rounding noise. This is standard practice whenever testing calculations with decimal numbers.

---

## 6. What `assert` does

`assert` is a Python keyword that says "this must be true, or crash". In normal code you rarely use it. In tests it is the primary tool.

```python
assert 2 + 2 == 4          # passes silently
assert 2 + 2 == 5          # raises AssertionError: assert 4 == 5
assert "sql" in vocab      # checks membership
assert df.count() == 3     # checks a Spark DataFrame row count
assert result is None      # checks for None
assert result is not None  # checks that something was returned
```

When an assert fails, pytest catches the `AssertionError` and prints a detailed failure report showing the actual values, not just "it failed".

---

## 7. Fixtures — the most important pytest concept

A **fixture** is a reusable piece of setup that multiple tests can share. In this project, the most important fixture is the Spark session:

```python
@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder.appName("test_phase3")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
```

**Breaking this down:**

`@pytest.fixture` — this decorator tells pytest "this is not a test, it is setup code that tests can request".

`scope="module"` — this controls how often the fixture is created:
- `scope="function"` (default) — a new Spark session for every single test. Very slow.
- `scope="module"` — one Spark session for the entire file. Created once, reused by all tests in the file, then shut down. This is what we use.
- `scope="session"` — one Spark session for the entire pytest run. Faster but can cause tests to interfere with each other.

We use `scope="module"` because starting a Spark session takes 10–15 seconds. If each of the 22 tests in `test_phase3_classification.py` created its own session, the file would take 4+ minutes to run. With one shared session it takes about 30 seconds.

`yield s` — this is the key pattern. Everything before `yield` is setup. `yield` hands the session to the test. Everything after `yield` is teardown (cleanup). When the module finishes, `s.stop()` shuts down the Spark session.

**How a test uses the fixture:**

```python
def test_year_filter(spark):   # <-- pytest sees "spark" and calls the fixture
    df = spark.createDataFrame(...)
```

The function parameter name `spark` must exactly match the fixture name. pytest wires them up automatically — you never call the fixture yourself.

**Why not just create the Spark session at the top of the file?**

```python
# BAD — do not do this
spark = SparkSession.builder...getOrCreate()

def test_something():
    spark.createDataFrame(...)
```

This creates the session at import time, even if you only want to run one test. The fixture is lazy — it only creates the session when a test actually needs it.

---

## 8. Spark-specific testing decisions

### Why `local[1]` instead of `local[4]`?

In tests, `local[1]` means one CPU thread. Tests run on tiny synthetic DataFrames (5–20 rows), not 62 million rows. Using more threads would not speed anything up and would waste resources. `local[4]` is for production where parallelism matters.

### Why `spark.sql.shuffle.partitions = 2`?

Spark's default is 200 shuffle partitions. On a 5-row test DataFrame this means 200 empty partitions are created for every groupBy or join operation. Setting it to 2 makes tests 5–10× faster with no downside.

### Why explicit schemas for DataFrames with `None`?

When Spark sees `[("job_001", None)]`, it cannot determine the type of the `None` column — is it a String? An Integer? This causes a `PySparkValueError`. You must tell Spark explicitly:

```python
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("job_id", StringType()),
    StructField("date", StringType()),   # explicit — so None is valid
])
df = spark.createDataFrame([("job_001", None)], schema)
```

This is also good practice in general — explicit schemas make tests self-documenting.

### Why import pyspark inside test functions, not at the top of the file?

```python
# Pattern used in this project
def test_something(spark):
    import pyspark.sql.functions as F
    df = spark.createDataFrame(...)
```

If pyspark is imported at module level and is not installed, the entire test file fails to load and all tests in it are skipped — including the pure Python ones that do not need Spark at all. Importing inside the function means only the tests that actually use Spark fail, and the others still run.

---

## 9. The `scope` of imports — why this caused one of the failures

One of the two failing tests had this bug:

```python
def test_something(spark):
    import pyspark.sql.functions as F
    rows = [
        ("noun_chunk", StringType()),   # BUG: StringType not imported yet
    ]
    from pyspark.sql.types import StructType, StructField, StringType, ...
```

Python executes code top to bottom. When it hits `StringType()` on line 3, it has not yet executed the `from pyspark.sql.types import ...` on line 6. So `StringType` is undefined and you get `UnboundLocalError`.

The fix is always to put imports at the top of the function, before any code that uses them:

```python
def test_something(spark):
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    # NOW you can use StringType
    schema = StructType([StructField("noun_chunk", StringType()), ...])
```

---

## 10. What we test and what we do not

**What is worth testing:**

- Mathematical formulas (investment = alpha × COMP_EMP × share) — these must be exactly right
- Threshold boundary conditions (at, above, below DATA_THRESHOLD) — off-by-one errors are common
- Logic that could silently produce wrong results (left join vs inner join)
- Regex patterns (SOC code extraction)
- Config invariants (ALL_ANCHOR_SOCS must match SOC_GROUPS)
- File operations (cleanup logic in Phase 0)
- Data cleaning rules (noun chunk cleaning)

**What is NOT worth testing:**

- That PySpark's `groupBy` works — it is a tested library, trust it
- That pandas reads a CSV — trust pandas
- That spaCy loads a model — trust spaCy
- The full pipeline end-to-end — this is what you do manually with real data

The principle is: test your logic, not the framework's logic.

---

## 11. Every test file in this project and what it covers

### `test_config.py` — no Spark, runs in seconds

Tests that the configuration in `config.py` is internally consistent before the pipeline even starts. These are the most important tests because a bad config produces wrong results silently — no crash, no error, just incorrect economics.

Key tests:
- `test_anchor_socs_are_subset_of_soc_groups` — catches the most common config mistake: adding a SOC code to `SOC_GROUPS` but forgetting to add it to `ALL_ANCHOR_SOCS`
- `test_alpha_map_covers_all_expected_sic_codes` — ensures no sector is missing a capital multiplier
- `test_sim_grounding_is_valid_cosine_range` — cosine similarity is bounded to [-1, 1], so a threshold above 1.0 would filter out everything

### `test_phase0_preflight.py` — no Spark, runs in seconds

Tests the cleanup logic that runs before the pipeline. Uses Python's `tempfile.TemporaryDirectory()` to create throwaway directories so tests never touch real files on disk.

Key pattern:
```python
def test_raw_dictionary_deleted_when_flag_is_false():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "oecd_dictionary_raw.csv")
        open(path, "w").close()           # create the file
        run_preflight_cleanup(False, ...)  # run the logic
        assert not os.path.exists(path)   # verify it was deleted
```

`tempfile.TemporaryDirectory()` creates a real directory on disk that is automatically deleted when the `with` block exits. Tests can create and delete files inside it without affecting anything else.

### `test_phase1_nlp.py` — mixed (cleaning tests are pure Python, schema/date tests use Spark)

Tests the noun chunk cleaning rules extracted from `extract_noun_chunks_packed`. The cleaning function is copied into the test file as a standalone function — this is the **extract-and-test pattern**: when logic is buried inside a complex function (like one that also runs spaCy), you extract just the piece you want to test.

Also tests Spark date operations and sampling behaviour.

### `test_phase2_dictionary.py` — uses Spark

Tests the core statistical logic: relative share calculation, the `explode(arrays_zip(...))` unpacking, and the triple filter. Uses small synthetic DataFrames where the correct answer can be computed by hand.

Key insight tested: `countDistinct` vs `count`:
```python
# "sql" appears 3 times in job_001 — should count as 1 job, not 3
rows = [("sql", "job_001", "3133")] * 3
result = df.groupBy("noun_chunk").agg(F.countDistinct("doc_JobID"))
assert result.first()["global_count"] == 1  # NOT 3
```

### `test_phase2_5_polish.py` — uses Spark

Tests the gold standard filtering. Because running actual spaCy inference is too slow for unit tests (it requires a large downloaded model and takes seconds per term), these tests use pre-scored DataFrames — synthetic data where `gold_sim_score` is already filled in.

This is a **stub** approach: you do not test spaCy's ability to compute similarity (trust the library), you test that your filtering threshold is applied correctly.

### `test_phase3_classification.py` — uses Spark

The most important test file in the project. Tests the classification logic including the critical left join behaviour.

The most important test:
```python
def test_inner_join_would_drop_zero_match_jobs(spark):
    """Demonstrates why left join is essential."""
    inner = all_jobs.join(classified, ..., "inner")
    left  = all_jobs.join(classified, ..., "left")
    assert inner.count() == 1  # job_002 dropped — WRONG
    assert left.count()  == 2  # job_002 preserved — CORRECT
```

This test exists not just to verify the code is right, but to document WHY the left join was chosen. If someone changes it to an inner join thinking it is equivalent, this test will immediately catch it and the failure message explains the consequence.

### `test_phase4_sector_mapping.py` — mixed

Tests the Census weight calculation. The most important property: weights must sum to exactly 1.0 for each SOC code. If they do not, you are either double-counting or under-counting workers in the sector distribution.

```python
def test_weights_sum_to_one_for_single_soc(spark):
    weight_sum = weights.groupBy("soc4").agg(F.sum("w")).first()["total_w"]
    assert abs(weight_sum - 1.0) < 1e-9
```

Also tests the SIC code grouping (pure Python — no Spark needed).

### `test_phase5_valuation.py` — mixed

Tests the economic formulae. Pure Python tests verify the formula itself. Spark tests verify that Spark applies it correctly across multiple sectors and years.

### `test_phase6_dashboard.py` — no Spark, runs in seconds

Tests HTML generation, base64 encoding, and file I/O. Uses the `DashboardBuilder` class — a minimal reimplementation of the dashboard logic that can be tested without running Plotly or generating a real chart.

### `test_noun_chunk_cleaning.py`, `test_sic_grouping.py`, `test_soc_extraction.py`, `test_economic_valuation.py`

Focused tests on specific functions. These existed before the phase-by-phase tests and cover a subset of the same ground — they can be thought of as a fast smoke test suite since they use no Spark.

---

## 12. Running the tests

**All tests:**
```bash
cd /Users/saurabhkumar/Desktop/OECD_PYSPARK_LOCAL/Production_ready
pytest
```

**All tests, verbose (see each test name):**
```bash
pytest -v
```

**One file:**
```bash
pytest tests/test_phase3_classification.py
```

**One specific test:**
```bash
pytest tests/test_phase3_classification.py::test_inner_join_would_drop_zero_match_jobs
```

**Only fast tests (no Spark):**
```bash
pytest tests/test_config.py tests/test_phase0_preflight.py tests/test_noun_chunk_cleaning.py tests/test_sic_grouping.py tests/test_soc_extraction.py tests/test_phase6_dashboard.py
```

**Stop at first failure:**
```bash
pytest -x
```

**Show slowest tests:**
```bash
pytest --durations=10
```

**See print() output from tests (useful for debugging):**
```bash
pytest -s
```

---

## 13. Understanding test output

**Passing run:**
```
tests/test_config.py::test_anchor_socs_are_subset_of_soc_groups PASSED
tests/test_config.py::test_soc_groups_not_empty PASSED
...
============= 256 passed in 52.3s =============
```

**Failing run:**
```
FAILED tests/test_config.py::test_anchor_socs_are_subset_of_soc_groups

tests/test_config.py:18: AssertionError
AssertionError: ALL_ANCHOR_SOCS contains codes not defined in SOC_GROUPS: {'3544', '2433'}.
Copy every SOC code from SOC_GROUPS into ALL_ANCHOR_SOCS.
```

The failure shows:
1. Which test failed
2. Which line it failed on
3. The assertion message — which in this project is written to explain what to do to fix the problem

---

## 14. How to write a new test

When you add a new feature or change existing logic, follow this process:

**Step 1 — Decide what the smallest testable unit is.**
If you are adding a new filter (e.g. "terms must have appeared in at least 3 different months"), the unit is: given a DataFrame with month information, does the filter retain the right terms?

**Step 2 — Write the test before or alongside the code (not after).**
Writing the test first forces you to think about what the correct behaviour actually is. This is called Test-Driven Development (TDD) and is considered best practice.

**Step 3 — Use the AAA pattern.**
```python
def test_new_month_filter(spark):
    # Arrange
    rows = [("sql", "job_001", 1), ("sql", "job_002", 2), ("sql", "job_003", 3)]
    df = spark.createDataFrame(rows, ["noun_chunk", "doc_JobID", "doc_month"])

    # Act
    result = df.groupBy("noun_chunk").agg(
        F.countDistinct("doc_month").alias("month_count")
    ).filter(F.col("month_count") >= 3)

    # Assert
    assert result.count() == 1
    assert result.first()["noun_chunk"] == "sql"
```

**Step 4 — Test the boundary, not just the happy path.**
Add a test where the term appears in only 2 months and verify it is excluded. Add a test where it appears in exactly 3 months and verify it is included. These boundary tests are where bugs hide.

**Step 5 — Put the test in the right file.**
- Logic related to Phase 1? → `test_phase1_nlp.py`
- Config-related? → `test_config.py`
- New standalone formula? → Create `test_new_feature.py`

---

## 15. What makes a good test name

Test names are documentation. The name should read like a sentence:

```python
# BAD — tells you nothing
def test_1():
def test_filter():
def test_works():

# GOOD — tells you exactly what is being verified
def test_term_absent_from_anchor_has_zero_relative_share():
def test_job_one_below_threshold_is_not_intensive():
def test_weights_sum_to_one_for_multiple_soc_codes():
```

When a test fails at 2am on a CI server, the name alone should tell you what broke without needing to read the code.

---

## 16. Test count breakdown

| File | Tests | Spark? |
|---|---|---|
| `test_config.py` | 9 | No |
| `test_pipeline_logic.py` | 8 | Yes |
| `test_noun_chunk_cleaning.py` | 14 | No |
| `test_sic_grouping.py` | 24 | No |
| `test_soc_extraction.py` | 9 | No |
| `test_economic_valuation.py` | 12 | No |
| `test_phase0_preflight.py` | 14 | No |
| `test_phase1_nlp.py` | 28 | Mixed |
| `test_phase2_dictionary.py` | 18 | Yes |
| `test_phase2_5_polish.py` | 18 | Mixed |
| `test_phase3_classification.py` | 22 | Yes |
| `test_phase4_sector_mapping.py` | 18 | Mixed |
| `test_phase5_valuation.py` | 22 | Mixed |
| `test_phase6_dashboard.py` | 24 | No |
| **Total** | **256** | |

---

## 17. What to do when a test fails

1. **Read the failure message carefully.** pytest shows you the exact line that failed and the actual values. Most of the time this tells you everything.

2. **Do not delete the test.** A failing test means either the code is wrong or the test is wrong. Figure out which before doing anything.

3. **Reproduce the failure in isolation.** Run just the failing test: `pytest tests/test_phase2_dictionary.py::test_failing_test -v -s`

4. **Check if the test itself has a bug.** The two failures in this project were both bugs in the test code (wrong import order, missing schema), not bugs in the pipeline.

5. **If the pipeline logic changed intentionally, update the test.** If you change `DATA_THRESHOLD` from 3 to 5, the tests that check the boundary around 3 will fail. That is expected — update them to reflect the new threshold.

6. **If the test found a real bug, fix the pipeline code.** Do not change the test to make it pass by adjusting the expected value.

---

## 18. Measuring test coverage

Coverage tells you what percentage of your code is executed by the tests. Install the coverage plugin:

```bash
pip install pytest-cov
```

Run with coverage:
```bash
pytest --cov=. --cov-report=term-missing
```

This shows you which lines of `main.py` and `config.py` are not covered by any test. 100% coverage does not mean 100% correct code, but low coverage means there are large areas of logic that have never been verified.

---

## 19. The two bugs that were fixed in this project

**Bug 1 — `UnboundLocalError` in `test_phase2_dictionary.py`:**

`StringType()` was used before `from pyspark.sql.types import StringType` was executed. Python reads functions top to bottom at runtime, so imports must come before the code that uses them.

**Root cause:** Copy-paste error — schema field definitions were accidentally placed in the `rows` list instead of the schema, and the import was placed after them.

**Bug 2 — `PySparkValueError` in `test_phase1_nlp.py`:**

`spark.createDataFrame([("job_001", None)], ["job_id", "date"])` fails because Spark cannot determine the type of `None`. Without a value to inspect, it cannot guess whether this column should be a StringType, IntegerType, or something else.

**Fix:** Always pass an explicit `StructType` schema when any value in your test data is `None`.

**Why document these?** Because you will encounter both of these errors again. When you do, you will remember where to look.
