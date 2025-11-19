# consumer.py
import json
import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_values

def run_consumer():
    print("[Consumer] Connecting to Kafka at localhost:9092...")
    consumer = KafkaConsumer(
        "trips",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="trips-consumer-group",
    )
    print("[Consumer] ✓ Connected to Kafka.")
    conn = psycopg2.connect(dbname="kafka_db", user="kafka_user", password="kafka_password", host="localhost", port="5432")
    conn.autocommit = True
    cur = conn.cursor()
    # Table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trips (
        trip_id VARCHAR(50) PRIMARY KEY,
        driver_id VARCHAR(50),
        rider_id VARCHAR(50),
        start_time TIMESTAMPTZ,
        end_time TIMESTAMPTZ,
        status VARCHAR(30),
        start_lat DOUBLE PRECISION,
        start_lon DOUBLE PRECISION,
        end_lat DOUBLE PRECISION,
        end_lon DOUBLE PRECISION,
        distance_km NUMERIC(8,2),
        duration_sec INTEGER,
        fare NUMERIC(10,2),
        payment_method VARCHAR(30),
        vehicle_type VARCHAR(30),
        updated_at TIMESTAMPTZ DEFAULT now()
    );
    """)
    print("[Consumer] ✓ Table ready. Listening...")
    for msg in consumer:
        try:
            ev = msg.value
            # parse possible None timestamps
            cur.execute("""
                INSERT INTO trips (trip_id, driver_id, rider_id, start_time, end_time, status,
                                   start_lat, start_lon, end_lat, end_lon, distance_km, duration_sec, fare,
                                   payment_method, vehicle_type, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                ON CONFLICT (trip_id) DO UPDATE SET
                    driver_id = EXCLUDED.driver_id,
                    rider_id = EXCLUDED.rider_id,
                    start_time = COALESCE(EXCLUDED.start_time, trips.start_time),
                    end_time = COALESCE(EXCLUDED.end_time, trips.end_time),
                    status = EXCLUDED.status,
                    start_lat = COALESCE(EXCLUDED.start_lat, trips.start_lat),
                    start_lon = COALESCE(EXCLUDED.start_lon, trips.start_lon),
                    end_lat = COALESCE(EXCLUDED.end_lat, trips.end_lat),
                    end_lon = COALESCE(EXCLUDED.end_lon, trips.end_lon),
                    distance_km = COALESCE(EXCLUDED.distance_km, trips.distance_km),
                    duration_sec = COALESCE(EXCLUDED.duration_sec, trips.duration_sec),
                    fare = COALESCE(EXCLUDED.fare, trips.fare),
                    payment_method = COALESCE(EXCLUDED.payment_method, trips.payment_method),
                    vehicle_type = COALESCE(EXCLUDED.vehicle_type, trips.vehicle_type),
                    updated_at = now();
            """,
            (
                ev.get("trip_id"),
                ev.get("driver_id"),
                ev.get("rider_id"),
                ev.get("start_time"),
                ev.get("end_time"),
                ev.get("status"),
                ev.get("start_lat"),
                ev.get("start_lon"),
                ev.get("end_lat"),
                ev.get("end_lon"),
                ev.get("distance_km"),
                ev.get("duration_sec"),
                ev.get("fare"),
                ev.get("payment_method"),
                ev.get("vehicle_type"),
            ))
            print(f"[Consumer] persisted trip {ev.get('trip_id')} status={ev.get('status')}")
        except Exception as e:
            print("[Consumer ERROR] ", e)
            continue

if __name__ == "__main__":
    run_consumer()
