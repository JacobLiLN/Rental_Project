import snowflake.connector
import datetime
import config

conn = snowflake.connector.connect(
    user=config.USER,
    password=config.PASSWORD,
    account=config.ACCOUNT,
    warehouse=config.WAREHOUSE,
    database=config.DATABASE,
    schema=config.SCHEMA
)

today = datetime.date.today().isoformat()

cursor = conn.cursor()

cursor.execute(f"""
COPY INTO raw.tenants (tenant_id, unit_key, move_in_date, move_out_date, load_ts)
FROM (
    SELECT $1, $2, $3, $4, CURRENT_TIMESTAMP()
    FROM @rental_stage_incremental/tenants/{today}/generated_tenants.csv
)
FILE_FORMAT = (type = 'CSV' field_optionally_enclosed_by='"' skip_header=1);
""")

result_tenants = cursor.fetchall()
print('Tenants load result:', result_tenants)


cursor.execute(f"""
COPY INTO raw.events (tenant_id, unit_key, event_type, dt_occurred, load_ts)
FROM (
    SELECT $1, $2, $3, $4, CURRENT_TIMESTAMP()
    FROM @rental_stage_incremental/events/{today}/generated_events.csv
)
FILE_FORMAT = (type = 'CSV' field_optionally_enclosed_by='"' skip_header=1);
""")

result_events = cursor.fetchall()
print('Events load result: ', result_events)

cursor.close()
conn.close()
