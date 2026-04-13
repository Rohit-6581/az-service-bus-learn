# src/producer/test_duplicate.py
#
# Purpose: Test duplicate detection on the orders-queue
# What to expect: Two sends with same message_id — consumer receives only ONE
#
# How it works:
#   - Broker tracks message_id for 5 minutes (PT5M in config.json)
#   - Second message with same message_id is silently dropped by broker
#   - Your application code never sees the duplicate

import json
import sys
import os
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.config import CONNECTION_STRING, QUEUE_NAME

from azure.servicebus import ServiceBusClient, ServiceBusMessage


def send_message(client, order: dict, attempt: int):
    """Send a single order message and print what was stamped on the envelope."""
    sender = client.get_queue_sender(queue_name=QUEUE_NAME)

    with sender:
        message = ServiceBusMessage(
            # ── BODY ──────────────────────────────────────
            body=json.dumps(order),

            # ── DUPLICATE DETECTION KEY ───────────────────
            # Same order_id = same message_id = broker drops second one
            message_id=f"order-{order['order_id']}",

            # ── CUSTOM ENVELOPE ───────────────────────────
            application_properties={
                "priority": "high" if order["amount"] > 500 else "normal",
                "region": order.get("region", "unknown"),
                "schema_version": "1.0"
            }
        )

        sender.send_messages(message)

        print(f"  📤 Attempt {attempt} — sent")
        print(f"     message_id : {message.message_id}")
        print(f"     priority   : {message.application_properties['priority']}")
        print(f"     region     : {message.application_properties['region']}")


def test_duplicate_detection():
    print("\n🧪 Duplicate Detection Test")
    print("=" * 45)
    print("Sending same order TWICE with identical message_id")
    print("Broker should silently drop the second one")
    print("Consumer should receive exactly ONE message")
    print("=" * 45)

    order = {
        "order_id": "9999",
        "customer_id": "cust-dupe",
        "customer": "TestUser",
        "amount": 750,
        "region": "jharkhand",
        "order_type": "food",
        "items": ["test-burger", "test-fries"]
    }

    client = ServiceBusClient.from_connection_string(CONNECTION_STRING)

    with client:
        print(f"\n📨 First send:")
        send_message(client, order, attempt=1)

        print(f"\n⏳ Waiting 2 seconds...")
        time.sleep(2)

        print(f"\n📨 Second send (same message_id):")
        send_message(client, order, attempt=2)

    print("\n" + "=" * 45)
    print("✅ Both sends completed from producer side")
    print("👉 Now run the consumer:")
    print("   python .\\src\\consumer\\receive_order.py")
    print("\n   Expected: Only ONE message arrives")
    print("   If TWO arrive: duplicate detection not enabled on queue")
    print("=" * 45)


if __name__ == "__main__":
    test_duplicate_detection()