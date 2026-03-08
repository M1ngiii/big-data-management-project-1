# Project 1 — ETL Pipeline Report

**Group J**

---

## 1. Correctness

### 1a. Row Counts

| Stage | Rows | Removed |
|---|---|---|
| Input (raw, 2 files: Jan + Feb 2025) | 7,052,769 | — |
| After cleaning + deduplication | 6,762,459 | 290,310 (4.12%) |
| Final output (after global merge dedup) | 6,762,459 | 0 |

> **Notes:** Cleaning and deduplication are fused into a single pass inside `transform_minimum()`. The 290,310 removed rows include records with null/invalid timestamps, zero or negative trip distances, invalid location IDs, and exact duplicates on the composite key `(source_file, pickup_ts, dropoff_ts, pickup_location_id, dropoff_location_id, passenger_count, trip_distance)`. The final global dedup pass produced no additional removals

**Output quality report (`fare_per_mile`):**

| Metric | Value |
|---|---|
| Null count | 0 |
| Min | −24,000.00 |
| Max | 539,612.73 |
| Mean | 20.87 |

> The extreme min (−$24,000/mile) and max ($539,612/mile) indicate outlier rows with anomalous `total_amount` values that passed the current cleaning rules. These rows are not invalid under the defined constraints (non-null timestamps, positive distance, valid location IDs), but may warrant additional outlier filtering in future iterations.

---

### 1b. Bad Row Examples and Cleaning Rules Applied

Three representative bad rows were sampled from the raw input before cleaning:

**Example 1 — Zero trip distance**

```
pickup_ts=2025-02-01 00:30:36, dropoff_ts=2025-02-01 00:31:28
pickup_location_id=4, dropoff_location_id=4
passenger_count=1, trip_distance=0.0
```

Rule applied: `trip_distance > 0.0` — a recorded distance of zero is not a valid trip; the row is removed.

---

**Example 2 — Identical pickup and dropoff timestamps**

```
pickup_ts=2025-02-01 00:01:10, dropoff_ts=2025-02-01 00:01:10
pickup_location_id=161, dropoff_location_id=141
passenger_count=1, trip_distance=1.14
```

Rule applied: `dropoff_ts > pickup_ts` — a zero-duration trip cannot be valid regardless of the recorded distance; the row is removed.

---

**Example 3 — Identical pickup and dropoff timestamps (different locations)**

```
pickup_ts=2025-02-01 00:32:48, dropoff_ts=2025-02-01 00:32:48
pickup_location_id=230, dropoff_location_id=142
passenger_count=2, trip_distance=1.4
```

Rule applied: `dropoff_ts > pickup_ts` — same as above; instantaneous trips are considered erroneous records and are removed.

---

## 2. Performance

### 2a. Runtime

| Phase | Spark jobs | Duration |
|---|---|---|
| Data load + input row count | 2–4 | ~0.2 s |
| Bad row sampling | 5 | ~0.1 s |
| Cleaning + deduplication (cache + count) | 6–9 | ~80 s |
| Zone enrichment (broadcast join) | — (fused into write) | — |
| Merge + global dedup count | 10–11 | ~6 s |
| Write enriched parquet | 12–13 | ~28 s |
| Quality report aggregation | 14–16 | ~0.3 s |
| **Total job runtime** | | **~2 min** |

---

### 2b. Spark Web UI Screenshots

**Screenshot 1 — Job summary (total job/stage time)**

![Spark UI Job Summary](report_assets/jobs.bmp)
---

**Screenshot 2 — Stage detail (shuffle read/write for join or aggregation)**

![Spark UI Stage Detail](report_assets/stages.bmp)

---

## 2c. Optimization Choices

### Optimization 1 — Reducing shuffle partitions

By default, Spark uses 200 shuffle partitions for wide transformations such as joins and aggregations. With a dataset of ~6.7 million rows on a local or small cluster, 200 partitions creates significant scheduling and task-launch overhead: most tasks finish in milliseconds and spend a disproportionate amount of time in driver and executor coordination. We reduced this to 16:

```python
# Reduce number of partitions, lesser overhead
spark.conf.set("spark.sql.shuffle.partitions", "16")
```

**What changed:** With 200 partitions, the shuffle stage for the zone enrichment join produced a long tail of near-empty tasks visible in the Spark UI stage timeline. Dropping to 16 consolidated that work into fewer, fuller tasks. The stage that previously showed hundreds of sub-10 ms tasks instead completed with a small number of tasks each processing a meaningful data slice, and per-stage scheduling overhead fell noticeably as a result.

---

### Optimization 2 — Enabling Adaptive Query Execution (AQE)

Spark's AQE framework allows the engine to re-optimise the physical plan at runtime, after it has seen actual shuffle statistics rather than relying purely on estimated row counts. We enabled AQE together with its partition coalescing feature:

```python
# Ensure AQE is enabled
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**What changed:** AQE's `coalescePartitions` feature automatically merges small post-shuffle partitions into larger ones based on actual data sizes observed at runtime. This complements the static partition reduction above: rather than guessing the right count upfront, AQE adjusts dynamically per stage. In practice, stages following a shuffle showed fewer, better-balanced tasks in the Spark UI compared to runs without AQE, and the join and aggregation phases completed with consistently shorter stage durations.
---

## 3. Scenario — `fare_per_mile` Quality Report

### Requirement

> Add `fare_per_mile = total_amount / trip_distance`. Set to null if `trip_distance <= 0`. Write a small JSON file `data/outbox/quality_report.json` with: count of null `fare_per_mile`, min, max, mean (excluding nulls). Must be recomputed on each run.

### Implementation

**Step 1 — Compute `fare_per_mile` during transformation**

The field is derived inside `transform_minimum()`, after cleaning has already guaranteed `trip_distance > 0` for all surviving rows. The `F.when` guard handles the null-if-zero requirement explicitly:

```python
df = df.withColumn(
    "fare_per_mile",
    F.when(F.col("trip_distance") > 0, F.col("total_amount") / F.col("trip_distance"))
    .otherwise(None)
)
```

Because cleaning filters out `trip_distance <= 0` before this line runs, the `.otherwise(None)` branch is a safety net for any edge cases that might survive in future data.

**Step 2 — Recompute the quality report on every run**

After the enriched output is written, the report is always recomputed by reading back the full output dataset — not just the new batch. This satisfies the "must be recomputed on each run" requirement, as the report reflects the complete accumulated dataset regardless of which files were new:

```python
output_for_report = read_existing_output(spark, OUTPUT_PATH)
if output_for_report is not None:
    report = output_for_report.agg(
        F.count(F.when(F.col("fare_per_mile").isNull(), 1)).alias("null_fare_per_mile"),
        F.min("fare_per_mile").alias("min_fare_per_mile"),
        F.max("fare_per_mile").alias("max_fare_per_mile"),
        F.avg("fare_per_mile").alias("mean_fare_per_mile"),
    ).collect()[0]

    quality = {
        "null_fare_per_mile": report["null_fare_per_mile"],
        "min_fare_per_mile":  report["min_fare_per_mile"],
        "max_fare_per_mile":  report["max_fare_per_mile"],
        "mean_fare_per_mile": report["mean_fare_per_mile"],
    }
    with open(os.path.join(OUTBOX_DIR, "quality_report.json"), "w") as f:
        json.dump(quality, f, indent=2)
```

### Output

The report written to `data/outbox/quality_report.json` after processing January and February 2025:

```json
{
  "null_fare_per_mile": 0,
  "min_fare_per_mile": -24000.0,
  "max_fare_per_mile": 539612.73125,
  "mean_fare_per_mile": 20.871422390335834
}
```

| Metric | Value |
|---|---|
| Null `fare_per_mile` | 0 |
| Min | −24,000.00 |
| Max | 539,612.73 |
| Mean | 20.87 |

Zero null values confirm that `fare_per_mile` was successfully computed for every row in the output — consistent with the cleaning rule that removes all rows where `trip_distance <= 0` before the field is derived. The negative minimum and very large maximum reflect outlier `total_amount` values in the source data; these are outside the scenario's scope but noted as potential targets for a future cleaning rule.
