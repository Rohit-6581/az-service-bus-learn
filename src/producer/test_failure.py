# src/producer/test_failure.py
#
# Purpose: Send an order that will deliberately fail processing
# What to expect:
#   - Consumer abandons the message 3 times
#   - delivery_count increases each attempt
#   - After MaxDeliveryCount (3) broker moves to Dead-letter Queue

import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.config import CONNECTION_STRING, QUEUE_NAME

from azure.servicebus import ServiceBusClient, ServiceBusMessage


def send_poison_order():
    print("\n☠️  Sending poison order — this will fail processing")
    print("=" * 45)

    # This order has a missing customer field
    # Our consumer will detect this and abandon it
    poison_order = {
        "order_id": "FAIL-001",
        "customer_id": "cust-broken",
        "customer": "BrokenUser",
        "amount": -999,          # ← invalid amount — will trigger failure
        "region": "jharkhand",
        "order_type": "food",
        "items": []              # ← empty items — will trigger failure
    }

    client = ServiceBusClient.from_connection_string(CONNECTION_STRING)

    with client:
        sender = client.get_queue_sender(queue_name=QUEUE_NAME)
        with sender:
            message = ServiceBusMessage(
                body=json.dumps(poison_order),
                message_id=f"order-{poison_order['order_id']}",
                application_properties={
                    "priority": "normal",
                    "region": poison_order["region"],
                    "schema_version": "1.0"
                }
            )
            sender.send_messages(message)
            print(f"  📤 Sent poison order: {poison_order['order_id']}")
            print(f"     amount : {poison_order['amount']} ← invalid")
            print(f"     items  : {poison_order['items']} ← empty")
            print(f"\n👉 Now run the consumer to watch it fail and retry")


if __name__ == "__main__":
    send_poison_order()