import json
import random
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
import os
from faker import Faker
from kafka import KafkaProducer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC             = os.getenv("KAFKA_TOPIC", "cart-events")
INTERVAL          = float(os.getenv("EVENT_INTERVAL_SECONDS", "0.5"))

fake = Faker()

PRODUCTS = [
    {"product_id": "P001", "product_name": "Wireless Headphones",  "category": "Electronics", "price": 89.99},
    {"product_id": "P002", "product_name": "Running Shoes",         "category": "Sports",      "price": 124.99},
    {"product_id": "P003", "product_name": "Coffee Maker",          "category": "Kitchen",     "price": 49.99},
    {"product_id": "P004", "product_name": "Python Crash Course",   "category": "Books",       "price": 29.99},
    {"product_id": "P005", "product_name": "Yoga Mat",              "category": "Sports",      "price": 34.99},
    {"product_id": "P006", "product_name": "Smart Watch",           "category": "Electronics", "price": 299.99},
    {"product_id": "P007", "product_name": "Blender",               "category": "Kitchen",     "price": 59.99},
    {"product_id": "P008", "product_name": "Backpack",              "category": "Travel",      "price": 79.99},
    {"product_id": "P009", "product_name": "Desk Lamp",             "category": "Home",        "price": 39.99},
    {"product_id": "P010", "product_name": "Gaming Mouse",          "category": "Electronics", "price": 49.99},
]


def make_event(user_id, session_id, event_type, product):
    return {
        "user_id":      user_id,
        "session_id":   session_id,
        "product_id":   product["product_id"],
        "product_name": product["product_name"],
        "category":     product["category"],
        "price":        round(product["price"] * random.uniform(0.9, 1.1), 2),
        "quantity":     random.randint(1, 5),
        "event_type":   event_type,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }


def build_session_flow():
    """
    Return an ordered list of event_types for one session.
    Session mix (per spec):
      90% → add_to_cart then silence          (abandoned)
       5% → checkout_start then silence       (payment drop-off)
       5% → checkout_complete                 (converted)
    """
    roll = random.random()
    if roll < 0.90:
        return ["page_view", "add_to_cart"]
    elif roll < 0.95:
        return ["page_view", "add_to_cart", "cart_view", "checkout_start"]
    else:
        return ["page_view", "add_to_cart", "cart_view", "checkout_start", "checkout_complete"]


def run():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"[producer] Connected to {BOOTSTRAP_SERVERS}, publishing to '{TOPIC}'")
    print(f"[producer] Interval: {INTERVAL}s   Ctrl-C to stop\n")

    session_count = 0

    while True:
        user_id    = str(uuid.uuid4())
        session_id = str(uuid.uuid4())
        product    = random.choice(PRODUCTS)
        flow       = build_session_flow()
        session_count += 1

        for event_type in flow:
            event = make_event(user_id, session_id, event_type, product)
            producer.send(TOPIC, value=event)
            print(
                f"[session {session_count:>5}] {event_type:<20} "
                f"user={user_id[:8]}  product={product['product_name']:<24} "
                f"${event['price']:.2f} x{event['quantity']}"
            )
            time.sleep(INTERVAL)

        producer.flush()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\n[producer] Stopped.")
