import pandas as pd
import random
import uuid
from datetime import datetime, timedelta
import json
import os

BASE_PATH = "./datav2"
os.makedirs(f"{BASE_PATH}/batch", exist_ok=True)
os.makedirs(f"{BASE_PATH}/streaming", exist_ok=True)

random.seed(42)

# -----------------------------
# DIMENSIONS INCREMENTAL GENERATOR
# -----------------------------

def generate_customers_incremental(start_key=1001, n=50):
    """Generate incremental customer dimension records"""
    return pd.DataFrame([{
        "customer_key": start_key + i,
        "customer_id": f"CUST{start_key + i:06d}",
        "gender": random.choice(["M", "F"]),
        "age_group": random.choice(["18-25", "26-35", "36-45", "46-60"]),
        "country": random.choice(["India", "USA", "UK"])
    } for i in range(n)])

def generate_products_incremental(start_key=1001, n=50):
    """Generate incremental product dimension records"""
    return pd.DataFrame([{
        "product_key": start_key + i,
        "product_id": f"PRD{start_key + i:06d}",
        "category": random.choice(["Electronics", "Clothing", "Footwear"]),
        "brand": random.choice(["Apple", "Samsung", "Nike", "Adidas"])
    } for i in range(n)])

def generate_stores_incremental(start_key=101, n=5):
    return pd.DataFrame([{
        "store_key": start_key + i,
        "store_id": f"STR{start_key + i:04d}",
        "store_type": random.choice(["Online", "Physical"]),
        "country": random.choice(["India", "USA"])
    } for i in range(n)])

# -----------------------------
# FACT SALES INCREMENTAL GENERATOR
# -----------------------------

def generate_fact_sales_incremental(n=15000, start_date=datetime(2025,1,1),
                                    customer_max=1050, product_max=1050, store_max=105, payment_max=6):
    """Generate incremental fact_sales records"""
    records = []
    for _ in range(n):
        order_date = start_date + timedelta(days=random.randint(0, 7))
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
    """Generate JSON streaming events from fact data"""
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
    print("Generating incremental dimension and fact data...")

    # Generate incremental dimensions
    cust_inc = generate_customers_incremental()
    prod_inc = generate_products_incremental()
    store_inc = generate_stores_incremental()

    cust_inc.to_csv(f"{BASE_PATH}/batch/dim_customer_incremental.csv", index=False)
    prod_inc.to_csv(f"{BASE_PATH}/batch/dim_product_incremental.csv", index=False)
    store_inc.to_csv(f"{BASE_PATH}/batch/dim_store_incremental.csv", index=False)

    # Generate incremental fact data
    fact_inc = generate_fact_sales_incremental()
    fact_inc.to_csv(f"{BASE_PATH}/batch/fact_sales_incremental.csv", index=False)

    # Generate streaming events
    generate_streaming_events(fact_inc, f"{BASE_PATH}/streaming/sales_events_incremental.json")

    print("✅ Incremental data generation complete.")
    print("Files generated:")
    print("- batch/dim_customer_incremental.csv")
    print("- batch/dim_product_incremental.csv")
    print("- batch/dim_store_incremental.csv")
    print("- batch/fact_sales_incremental.csv")
    print("- streaming/sales_events_incremental.json")
