# 🚀 Airflow Data Quality Pipeline

## 📌 Overview

This project demonstrates a production-style data pipeline built using Apache Airflow.

The pipeline generates booking data, validates data quality, and separates:
- ✅ Clean data
- ❌ Anomalies (bad data)

---

## 🧠 Problem Statement

In real-world data engineering, raw data often contains:
- Missing fields
- Invalid values
- Inconsistent formats

If not handled properly, this leads to incorrect analytics and poor business decisions.

---

## ⚙️ Solution

This pipeline introduces a **data quality layer** that ensures only clean and valid data is used downstream.

---


## 🏗️ Pipeline Flow

```mermaid
graph LR
    A[generate_bookings] --> B[quality_check]
    B --> C[clean_data]
    B --> D[anomalies]
    
    style A fill:#4CAF50,stroke:#2E7D32,stroke-width:2px,color:#fff
    style B fill:#2196F3,stroke:#0b5e7e,stroke-width:2px,color:#fff
    style C fill:#FFC107,stroke:#FF6F00,stroke-width:2px,color:#000
    style D fill:#f44336,stroke:#c62828,stroke-width:2px,color:#fff
```

## 🛠️ Tools Used

- Apache Airflow
- Python
- JSON

---

## 🔍 Key Features

- Automated data pipeline using Airflow DAG
- Data validation for missing and invalid fields
- Separation of clean vs bad data
- Dynamic file partitioning using execution date

---

## 📊 Sample Output

- [Clean Data Sample](sample_output/clean_sample.json)
- [Anomalies Sample](sample_output/anomalies_sample.json)

---

## 🧪 Screenshots

### Airflow DAG (Graph View)

<img width="1913" height="861" alt="Screenshot 2026-03-26 152630" src="https://github.com/user-attachments/assets/35e3753d-bd51-485c-a073-2873cc2c7fc6" />

### Successful Pipeline Run

<img width="1914" height="670" alt="Screenshot 2026-03-26 154313" src="https://github.com/user-attachments/assets/c269a396-1ff8-4c6d-b57c-bbc16ed1ad11" />

---

## 🔗 Where Spark Fits (Future Extension)

In a real-world pipeline, this system can be extended as:
```mermaid
graph LR
    A[S3] --> B[Spark EMR]
    B --> C[Airflow]
    C --> D[Data Quality]
    D --> E[Athena]
    
    style A fill:#FF9900,stroke:#232F3E
    style B fill:#FF4F8B,stroke:#99002E,color:#fff
    style C fill:#0174CE,stroke:#0056A3,color:#fff
    style D fill:#28A745,stroke:#1E6F2F,color:#fff
    style E fill:#7B42F5,stroke:#4A1D96,color:#fff
```

- Spark handles large-scale data processing
- Airflow orchestrates the workflow
- This project represents the **data validation layer**

---

## 📚 What I Learned

- Building Airflow DAGs
- Task dependencies and orchestration
- Debugging using Airflow logs
- Handling Airflow version changes (`execution_date` → `logical_date`)
- Importance of data quality in pipelines

---

## 🚀 Future Improvements

- Store data in AWS S3
- Integrate Spark (EMR) for processing
- Query results using AWS Athena

---

## 👤 Author

Built as part of my Data Engineering learning journey.


   

