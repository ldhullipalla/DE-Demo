import pandas as pd
import random
import uuid
from datetime import datetime, timedelta
import json
import os

BASE_PATH = "./data"
os.makedirs(f"{BASE_PATH}/batch", exist_ok=True)
os.makedirs(f"{BASE_PATH}/streaming", exist_ok=True)

random.seed(42)

# -----------------------------
# DIMENSIONS
# -----------------------------

def generate_customers(n=1000):
    return pd.DataFrame([{
        "customer_key": i,
        "customer_id": f"CUST{i:06d}",
        "gender": random.choice(["M", "F"]),
        "age_group": random.choice(["18-25", "26-35", "36-45", "46-60"]),
        "country": random.choice(["India", "USA", "UK"])
    } for i in range(1, n+1)])

def generate_products(n=1000):
    return pd.DataFrame([{
        "product_key": i,
        "product_id": f"PRD{i:06d}",
        "category": random.choice(["Electronics", "Clothing", "Footwear"]),
        "brand": random.choice(["Apple", "Samsung", "Nike", "Adidas"])
    } for i in range(1, n+1)])

def generate_stores(n=100):
    return pd.DataFrame([{
        "store_key": i,
        "store_id": f"STR{i:04d}",
        "store_type": random.choice(["Online", "Physical"]),
        "country": random.choice(["India", "USA"])
    } for i in range(1, n+1)])

def generate_payment():
    return pd.DataFrame([
        {"payment_key": 1, "payment_type": "CREDIT"},
        {"payment_key": 2, "payment_type": "DEBIT"},
        {"payment_key": 3, "payment_type": "UPI"},
        {"payment_key": 4, "payment_type": "CASH"},
        {"payment_key": 5, "payment_type": "WALLET"},
        {"payment_key": 6, "payment_type": "NETBANKING"}
    ])

def generate_date_dim(years=3):
    start = datetime(2023, 1, 1)
    days = years * 365
    return pd.DataFrame([{
        "date_key": int((start + timedelta(days=i)).strftime("%Y%m%d")),
        "full_date": (start + timedelta(days=i)).date(),
        "year": (start + timedelta(days=i)).year,
        "month": (start + timedelta(days=i)).month,
        "day": (start + timedelta(days=i)).day,
        "weekday": (start + timedelta(days=i)).strftime("%A")
    } for i in range(days)])

# -----------------------------
# FACT SALES GENERATOR
# -----------------------------

def generate_fact_sales(n, start_date, customer_max=1000, product_max=1000, store_max=100, payment_max=6):
    """Generates fact_sales dataframe"""
    records = []
    for _ in range(n):
        order_date = start_date + timedelta(days=random.randint(0, 30))
        qty = random.randint(1, 5)
        price = random.randint(500, 90000)
        records.append({
            "order_id": f"ORD{uuid.uuid4().hex[:10]}",
            "order_date": order_date.date(),
            "customer_key": random.randint(1, customer_max),
            "product_key": random.randint(1, product_max),
            "store_key": random.randint(1, store_max),
            "payment_key": random.randint(1, payment_max),
            "quantity": qty,
            "unit_price": price,
            "sales_amount": qty * price
        })
    return pd.DataFrame(records)

# -----------------------------
# STREAMING EVENTS GENERATOR
# -----------------------------

def generate_streaming_events(fact_df, path):
    events = []
    for _, row in fact_df.iterrows():
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_type": "ORDER_PLACED",
            "event_time": datetime.utcnow().isoformat(),
            "order_id": row["order_id"],
            "customer_id": f"CUST{row['customer_key']:06d}",
            "product_id": f"PRD{row['product_key']:06d}",
            "store_id": f"STR{row['store_key']:04d}",
            "quantity": row["quantity"],
            "unit_price": row["unit_price"],
            "channel": random.choice(["ONLINE", "STORE"])
        })

    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

# -----------------------------
# MAIN EXECUTION
# -----------------------------

if __name__ == "__main__":

    # ---------------- HISTORY LOAD ----------------
    print("Generating historical load...")
    customer_df = generate_customers()
    product_df = generate_products()
    store_df = generate_stores()
    payment_df = generate_payment()
    date_df = generate_date_dim()

    # Save historical batch CSVs
    customer_df.to_csv(f"{BASE_PATH}/batch/dim_customer.csv", index=False)
    product_df.to_csv(f"{BASE_PATH}/batch/dim_product.csv", index=False)
    store_df.to_csv(f"{BASE_PATH}/batch/dim_store.csv", index=False)
    payment_df.to_csv(f"{BASE_PATH}/batch/dim_payment.csv", index=False)
    date_df.to_csv(f"{BASE_PATH}/batch/dim_date.csv", index=False)

    fact_history_df = generate_fact_sales(n=100000, start_date=datetime(2023,1,1))
    fact_history_df.to_csv(f"{BASE_PATH}/batch/fact_sales.csv", index=False)
    generate_streaming_events(fact_history_df, f"{BASE_PATH}/streaming/sales_events_history.json")

    # ---------------- INCREMENTAL LOAD ----------------
    print("Generating incremental load...")
    fact_incremental_df = generate_fact_sales(n=15000, start_date=datetime(2025,1,1))
    fact_incremental_df.to_csv(f"{BASE_PATH}/batch/fact_sales_incremental.csv", index=False)
    generate_streaming_events(fact_incremental_df, f"{BASE_PATH}/streaming/sales_events_incremental.json")

    print("✅ Data generation complete.")
