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

### 2c. Optimization Choices

**Optimization 1 — Broadcast join for zone lookup**

The taxi zone lookup table (~265 rows) is small enough to fit in memory on every executor. Rather than a standard shuffle join (which would require redistributing both the trip data and the lookup table across the network), we explicitly wrap the lookup DataFrame in `F.broadcast()`:

```python
df = df.join(F.broadcast(pu), on="pickup_location_id", how="left")
df = df.join(F.broadcast(do), on="dropoff_location_id", how="left")
```

This eliminates shuffle for both the pickup and dropoff zone joins. In the Spark UI, the corresponding stages show near-zero shuffle read/write, compared to a standard join which would produce shuffle traffic proportional to the full 7M-row dataset.

**Observed change:** _TODO — e.g., "Stage 4 shuffle write dropped from X MB to 0 MB; stage duration dropped from Xs to Ys."_

---

**Optimization 2 — Caching the cleaned DataFrame**

After `transform_minimum()`, the cleaned DataFrame is cached with `.cache()` before the row count action:

```python
new_clean_df = transform_minimum(new_raw_df).cache()
after_clean_rows = new_clean_df.count()
```

Without caching, Spark would recompute the full cleaning pipeline (including the source parquet reads) twice: once for the `count()` and again for the downstream enrichment and join. Caching materialises the cleaned data in memory after the first pass, so subsequent operations reuse it without re-scanning or re-cleaning the raw files.

**Observed change:** _TODO — e.g., "Second pass over cleaned data served from cache in Xs vs Ys for a full recompute; visible in UI as a cached RDD reuse on stage X."_

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
