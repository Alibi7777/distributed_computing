Here is a **clean, ready-to-submit README.md** you can copy directly into your repository.
It matches exactly what you ran and what you showed in the defense.

---

# LAB 5 — Mini-MapReduce on Amazon EMR

## Overview

This project demonstrates a **distributed MapReduce WordCount pipeline** running on **Amazon Elastic MapReduce (EMR)** using **Hadoop Streaming** and **Python**.

The job processes a large text dataset stored in **HDFS**, executes mapper and reducer tasks across multiple nodes, and writes aggregated results back to HDFS.

---

## Technology Stack

* Amazon EMR
* Hadoop MapReduce
* Hadoop Streaming
* HDFS
* Python 3
* SSH / AWS Console

---

## Dataset Description

The dataset used in this lab is a **Simple English Wikipedia text dump**.

* **Source:**
  [https://github.com/LGDoor/Dump-of-Simple-English-Wiki](https://github.com/LGDoor/Dump-of-Simple-English-Wiki)
* **File:** `corpus.tgz`
* **Extracted file:** `corpus.txt`
* **Size:** ~32 MB (plain text)

The dataset contains Wikipedia articles in plain text format and is suitable for large-scale text processing tasks such as word counting.

---

## Files in Repository

```
.
├── mapper.py      # Python mapper script
├── reducer.py     # Python reducer script
└── README.md
```

---

## Mapper Logic (`mapper.py`)

* Reads input text line by line
* Splits lines into words
* Emits `(word, 1)` for each word

---

## Reducer Logic (`reducer.py`)

* Receives grouped `(word, count)` pairs
* Aggregates counts per word
* Outputs final `(word, total_count)`

---

## Running the Job on EMR

### 1. Upload input data to HDFS

```bash
hdfs dfs -mkdir -p /user/hadoop/input
hdfs dfs -put corpus.txt /user/hadoop/input/
```

### 2. Make scripts executable

```bash
chmod +x mapper.py reducer.py
```

### 3. Run Hadoop Streaming WordCount

```bash
hadoop jar /usr/lib/hadoop/hadoop-streaming.jar \
-files mapper.py,reducer.py \
-mapper mapper.py \
-reducer reducer.py \
-input /user/hadoop/input/ \
-output /user/hadoop/output/
```

---

## Output Validation

```bash
hdfs dfs -ls /user/hadoop/output/
hdfs dfs -head /user/hadoop/output/part-00000
```

The output directory contains:

* `_SUCCESS` (job completion indicator)
* Multiple `part-0000*` files with word counts

---

## Experimentation

**Scenario B — Input Size Comparison**

* Small dataset (~200,000 lines)
* Full dataset (~32 MB)

Observed that runtime differences are small due to Hadoop startup and shuffle overhead dominating at this data scale, while the job still executes in a fully distributed manner.

---

## Conclusion

This lab successfully demonstrates:

* Distributed MapReduce execution on EMR
* HDFS-based input and output
* Parallel processing using Hadoop Streaming
* Basic performance analysis using different input sizes

