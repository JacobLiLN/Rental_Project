import psycopg2
from faker import Faker
import random
import datetime
from datetime import timedelta
import config
import csv
import boto3

faker = Faker()

conn = psycopg2.connect(
    host=config.DB_HOST,
    port=config.DB_PORT,
    database=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASSWORD
)

def generate_tenant_id(existing_ids):
    while True:
        tenant_id = f"T{random.randint(50000,999999)}"
        if tenant_id not in existing_ids:
            return tenant_id

def generate_new_tenants(conn, num_new_tenants=10):
    cur = conn.cursor()
    # Step 1: Get current tenant count and dates by unit
    cur.execute("""
        SELECT unit_key, tenant_id, move_in_date, move_out_date
        FROM tenants
    """)
    tenant_rows = cur.fetchall()

    unit_tenants = {}
    for unit_key, tenant_id, move_in, move_out in tenant_rows:
        unit_tenants.setdefault(unit_key, []).append({
            "tenant_id": tenant_id,
            "move_in": move_in,
            "move_out": move_out
        })

    # Step 2: Filter units with fewer than 3 active tenants
    eligible_units = []
    for unit_key, records in unit_tenants.items():
        active = [t for t in records if not t["move_out"] or t["move_out"] > datetime.datetime.now().date()]
        if len(active) < 3:
            eligible_units.append((unit_key, active))

    if not eligible_units:
        print("No eligible units available for new tenants.")
        return []
    
    # Step 3: Generate new tenants
    new_tenants = []
    for _ in range(num_new_tenants):
        unit_key, active_tenants = random.choice(eligible_units)
        tenant_id = generate_tenant_id({t[0] for t in tenant_rows})
        move_in_date = max([t["move_in"] for t in active_tenants], default=datetime.datetime.now().date())
        move_in_date += timedelta(days=random.randint(1, 30))
        move_out_date = None  # Set this later if you want

        new_tenants.append((tenant_id, unit_key, move_in_date, move_out_date))

        # Update active_tenants count for this unit
        unit_tenants[unit_key].append({
            "tenant_id": tenant_id,
            "move_in": move_in_date,
            "move_out": move_out_date
        })
        if len([t for t in unit_tenants[unit_key] if not t["move_out"] or t["move_out"] > datetime.datetime.now().date()]) >= 3:
            eligible_units.remove((unit_key, active_tenants))
    
    # conn.commit()
    print(f"{len(new_tenants)} new tenants inserted.")

    return new_tenants


def generate_events_for_tenants(conn, new_tenants):
    cursor = conn.cursor()
    event_types = ['LATE_PAYMENT_NOTICE', 'INSPECTION', 'MAINTENANCE']
    event_records = []

    for tenant_id, unit_key, move_in, move_out in new_tenants:
        # Always add MOVE_IN
        event_records.append((tenant_id, unit_key, 'MOVE_IN', move_in))

        # Add MOVE_OUT if present
        if move_out:
            event_records.append((tenant_id, unit_key, 'MOVE_OUT', move_out))

        # Add 1–3 random intermediate events
        n_extra_events = random.randint(1, 3)
        for _ in range(n_extra_events):
            event_type = random.choice(event_types)
            end_date = move_out or datetime.date.today()
            if move_in >= end_date:
                continue  # skip generating this extra event
            event_date = faker.date_between(start_date=move_in, end_date=end_date)

            event_records.append((tenant_id, unit_key, event_type, event_date))
    
    return event_records


# Step 1: Generate tenants (but don't commit inside the function!)
new_tenants = generate_new_tenants(conn, num_new_tenants=10)

# Step 2: Generate related events (also don't commit in function!)
event_records = generate_events_for_tenants(conn, new_tenants)

# Step 3: Save to CSV
with open('generated_tenants.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['tenant_id', 'unit_key', 'move_in', 'move_out'])
    writer.writerows(new_tenants)

with open('generated_events.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['tenant_id', 'unit_key', 'event_type', 'dt_occurred'])
    writer.writerows(event_records)

print("CSV export complete.")




def upload_to_s3(local_file_path, bucket_name, prefix_folder):
    s3 = boto3.client("s3")
    today = datetime.date.today().isoformat()
    s3_key = f"{prefix_folder}/{today}/{local_file_path}"
    
    try:
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"Uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {local_file_path}: {e}")

# rental-data-batch-project

upload_to_s3("generated_tenants.csv", "rental-data-batch-project", "tenants")
upload_to_s3("generated_events.csv", "rental-data-batch-project", "events")