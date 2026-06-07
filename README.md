# End-to-End RAG Pipeline using PySpark, BigQuery and Vertex AI

## Project Overview

This project demonstrates an end-to-end Retrieval-Augmented Generation (RAG) pipeline built on Google Cloud Platform using Data Engineering and Generative AI components.

The pipeline ingests raw sustainability-related text documents from Google Cloud Storage (GCS), performs distributed text transformation and chunking using PySpark on Dataproc, stores processed chunks in BigQuery, and finally enables semantic retrieval and question-answering using embeddings, FAISS vector search, and a Large Language Model (LLM).

---

# Architecture

Raw TXT Files  
↓  
Google Cloud Storage (GCS)  
↓  
PySpark ETL Job on Dataproc  
↓  
BigQuery Data Warehouse  
↓  
Vertex AI Workbench Notebook  
↓  
Embeddings + FAISS Vector Search  
↓  
RAG-based Question Answering

---

# Technologies Used

- Google Cloud Storage (GCS)
- Dataproc
- PySpark
- BigQuery
- Vertex AI Workbench
- FAISS
- Sentence Transformers / Embedding Models
- Gemini / LLM-based Q&A
- Python

---

# Project Workflow

## 1. Raw Data Storage

Raw sustainability-related `.txt` files were uploaded to a Google Cloud Storage bucket.

Example:
- `sustainable_development_goals_report_2025.txt`

---

## 2. Distributed ETL using PySpark on Dataproc

A PySpark job was submitted to a Dataproc cluster to process the raw text files.

### Transformations Performed

- Text cleaning
- Lowercasing
- Regex cleanup
- Word tokenization
- Chunk generation
- Positional indexing using `posexplode`
- Chunk grouping
- Window functions
- Joins with metadata
- Character count filtering

### Additional Spark Concepts Demonstrated

- `posexplode`
- `groupBy`
- `collect_list`
- `concat_ws`
- Window functions
- DataFrame joins

---

## 3. Loading Processed Data into BigQuery

Processed document chunks were loaded into a BigQuery table using the Spark BigQuery connector.

### Example Schema

| Column | Description |
|---|---|
| chunk_id | Unique chunk identifier |
| chunk_text | Processed text chunk |
| chunk_sequence | Chunk order |
| topic | Document topic |
| country | Metadata |
| char_count | Chunk size |

---

## 4. Retrieval-Augmented Generation (RAG)

A Vertex AI Workbench notebook was used to build the RAG pipeline.

### Steps Performed

1. Read chunked data from BigQuery
2. Generate embeddings for text chunks
3. Store vectors in FAISS index
4. Perform semantic similarity search
5. Retrieve relevant chunks for user queries
6. Generate contextual answers using an LLM

---

# Example Questions

- What are sustainable development goals?
- How does renewable energy help sustainability?
- What are major climate challenges?
- Why is sustainable development important?

---

# Sample RAG Flow

User Question  
↓  
Embedding Generation  
↓  
FAISS Similarity Search  
↓  
Relevant Chunk Retrieval  
↓  
LLM-based Answer Generation

---

# Key Learnings

- Distributed text processing using PySpark
- Building scalable ETL pipelines on GCP
- Using BigQuery as an analytical data warehouse
- Understanding embeddings and vector search
- Implementing semantic retrieval using FAISS
- Building a basic Retrieval-Augmented Generation pipeline

---

# Repository Structure

```text
rag-data-engineering-pipeline/
│
├── dataproc/
│   └── gcs_to_bq.py
│
├── notebook/
│   └── rag_pipeline.ipynb
│
├── sample_data/
│
├── requirements.txt
│
├── README.md
│
└── .gitignore
````

---

# Future Improvements

* Deploy chatbot using Cloud Run
* Use Vertex AI Vector Search
* Add Streamlit frontend
* Add metadata-based filtering
* Add hybrid search capabilities
* Store embeddings in BigQuery

---
# DEMO

* RAW file in GCS bucket
<img width="975" height="399" alt="image" src="https://github.com/user-attachments/assets/dd89f48a-ea22-4ec3-8771-4b3e676ccbf6" />

* After pyspark dataproc job, transformed data gets ingested to Bigquery
<img width="975" height="353" alt="image" src="https://github.com/user-attachments/assets/484aaf3f-26ca-4ec0-9316-0bea92682094" />

<img width="975" height="423" alt="image" src="https://github.com/user-attachments/assets/11adf723-3670-43be-9fd3-e85a884f82e8" />

* Create  agent platform ai workbench instance:
<img width="975" height="403" alt="image" src="https://github.com/user-attachments/assets/6c1bf202-8734-4530-8cce-a17636046e69" />

* Response from Vertex AI workbench instance RAG notebook:


<img width="975" height="465" alt="image" src="https://github.com/user-attachments/assets/ed7e8826-f378-48fe-9638-e781c1e744d3" />



# Author

Ashley Dsouza

```
```
