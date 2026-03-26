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
    
   
