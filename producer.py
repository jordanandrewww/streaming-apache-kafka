# producer.py
import time, json, uuid, random
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker
import math

fake = Faker()

def random_point(center_lat, center_lon, max_km=10):
    # approx conversion
    r = max_km / 111.0
    u = random.random()
    v = random.random()
    w = r * math.sqrt(u)
    t = 2 * math.pi * v
    lat = center_lat + w * math.cos(t)
    lon = center_lon + w * math.sin(t) / math.cos(center_lat * math.pi/180)
    return round(lat, 6), round(lon, 6)

def generate_trip_event(in_progress=False):
    vehicle_types = ["sedan", "suv", "motorbike"]
    payment_methods = ["card", "cash", "wallet"]
    statuses = ["requested", "accepted", "in_progress", "completed", "cancelled"]

    trip_id = str(uuid.uuid4())[:8]
    driver_id = f"driver_{random.randint(1,200)}"
    rider_id = f"rider_{random.randint(1,1000)}"
    center_lat, center_lon = -23.55, -46.63  # São Paulo center
    s_lat, s_lon = random_point(center_lat, center_lon, max_km=12)
    e_lat, e_lon = random_point(center_lat, center_lon, max_km=12)

    distance_km = round(random.uniform(0.5, 25.0), 2)
    duration_sec = int(distance_km / 0.4 * 60)  # naive: avg 0.4km/min -> minutes->sec
    fare = round(3.5 + distance_km * random.uniform(0.9, 1.7), 2)

    # choose status: many will be in_progress/completed
    status = random.choices(statuses, weights=[5,10,35,40,10], k=1)[0]
    now = datetime.now(timezone.utc).isoformat()

    event = {
        "trip_id": trip_id,
        "driver_id": driver_id,
        "rider_id": rider_id,
        "start_time": now if status in ("requested", "accepted", "in_progress", "completed") else None,
        "end_time": now if status == "completed" else None,
        "status": status,
        "start_lat": s_lat,
        "start_lon": s_lon,
        "end_lat": e_lat if status=="completed" else None,
        "end_lon": e_lon if status=="completed" else None,
        "distance_km": distance_km,
        "duration_sec": duration_sec if status=="completed" else None,
        "fare": fare if status in ("completed",) else None,
        "payment_method": random.choice(payment_methods),
        "vehicle_type": random.choice(vehicle_types),
    }
    return event

def run_producer():
    print("[Producer] Connecting to Kafka at localhost:9092...")
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_block_ms=60000,
        retries=5,
    )
    print("[Producer] ✓ Connected to Kafka successfully!")
    count = 0
    try:
        while True:
            event = generate_trip_event()
            key = event["trip_id"].encode("utf-8")
            future = producer.send("trips", value=event, key=key)
            meta = future.get(timeout=10)
            print(f"[Producer] #{count} sent trip {event['trip_id']} status={event['status']} partition={meta.partition} offset={meta.offset}")
            producer.flush()
            count += 1
            time.sleep(random.uniform(0.2, 1.5))
    except KeyboardInterrupt:
        print("Stopping producer")
    except Exception as e:
        print("Producer Error:", e)
        raise

if __name__ == "__main__":
    run_producer()
