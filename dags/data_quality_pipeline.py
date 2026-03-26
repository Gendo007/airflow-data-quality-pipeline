from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime
import os
import json
import random


@dag(
    dag_id="data_quality_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    description="Data Quality Pipeline with Clean and Anomaly Separation",
)
def data_quality_pipeline():

    CORRECT_PROB = 0.7

    # ============================================================
    # PATH FUNCTIONS
    # ============================================================

    def get_bookings_path(context):
        execution_date = context["logical_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H-%M")
        return f"/tmp/data/bookings/{file_date}/bookings.json"

    def get_anomalies_path(context):
        execution_date = context["logical_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H-%M")
        return f"/tmp/data/anomalies/{file_date}/anomalies.json"

    def get_clean_data_path(context):
        execution_date = context["logical_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H-%M")
        return f"/tmp/data/clean/{file_date}/clean_bookings.json"

    # ============================================================
    # DATA GENERATORS
    # ============================================================

    def generate_booking_id(i):
        return i + 1 if random.random() < CORRECT_PROB else None

    def generate_listing_id():
        return random.choice([1, 2, 3, 4, 5]) if random.random() < CORRECT_PROB else None

    def generate_user_id():
        return random.randint(1000, 5000) if random.random() < CORRECT_PROB else None

    def generate_booking_time(execution_date):
        return execution_date.strftime("%Y-%m-%d %H:%M:%S") if random.random() < CORRECT_PROB else None

    def generate_status():
        if random.random() < CORRECT_PROB:
            return random.choice(["confirmed", "pending", "cancelled"])
        return random.choice(["invalid", "expired", None])

    # ============================================================
    # TASK 1: GENERATE BOOKINGS
    # ============================================================

    @task
    def generate_bookings():
        context = get_current_context()
        booking_path = get_bookings_path(context)

        num_bookings = random.randint(5, 15)
        bookings = []

        for i in range(num_bookings):
            booking = {
                "booking_id": generate_booking_id(i),
                "listing_id": generate_listing_id(),
                "user_id": generate_user_id(),
                "booking_time": generate_booking_time(context["logical_date"]),
                "status": generate_status(),
            }
            bookings.append(booking)

        directory = os.path.dirname(booking_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(booking_path, "w") as f:
            json.dump(bookings, f, indent=4)

        print(f"✅ Generated {num_bookings} bookings")
        print(f"📁 Saved to: {booking_path}")

        return booking_path

    # ============================================================
    # TASK 2: QUALITY CHECK
    # ============================================================

    @task
    def quality_check(booking_path: str):
        context = get_current_context()

        anomalies = []
        clean_data = []
        valid_statuses = {"confirmed", "pending", "cancelled"}

        with open(booking_path, "r") as f:
            bookings = json.load(f)

        for index, row in enumerate(bookings):
            row_anomalies = []

            if not row.get("booking_id"):
                row_anomalies.append("Missing booking_id")

            if not row.get("listing_id"):
                row_anomalies.append("Missing listing_id")

            if not row.get("user_id"):
                row_anomalies.append("Missing user_id")

            if not row.get("booking_time"):
                row_anomalies.append("Missing booking_time")

            if not row.get("status"):
                row_anomalies.append("Missing status")

            if row.get("status") and row["status"] not in valid_statuses:
                row_anomalies.append(f"Invalid status: {row['status']}")

            if row_anomalies:
                anomalies.append({
                    "booking_id": index,
                    "anomalies": row_anomalies,
                })
            else:
                clean_data.append(row)

        # ========================
        # SAVE ANOMALIES
        # ========================
        anomalies_file = get_anomalies_path(context)
        directory = os.path.dirname(anomalies_file)

        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(anomalies_file, "w") as f:
            json.dump(anomalies, f, indent=4)

        # ========================
        # SAVE CLEAN DATA
        # ========================
        clean_file = get_clean_data_path(context)
        directory = os.path.dirname(clean_file)

        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(clean_file, "w") as f:
            json.dump(clean_data, f, indent=4)

        print(f"🔍 Validation completed")
        print(f"⚠️ Anomalies: {len(anomalies)}")
        print(f"✅ Clean records: {len(clean_data)}")
        print(f"📁 Anomalies saved to: {anomalies_file}")
        print(f"📁 Clean data saved to: {clean_file}")

        return {
            "anomalies_file": anomalies_file,
            "clean_file": clean_file
        }

    # ============================================================
    # DAG FLOW
    # ============================================================

    booking_path = generate_bookings()
    quality_check(booking_path)


# ============================================================
# INSTANTIATE DAG
# ============================================================

dag_instance = data_quality_pipeline()
