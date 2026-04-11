# src/consumer/receive_order.py

import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.config import CONNECTION_STRING, QUEUE_NAME

from azure.servicebus import ServiceBusClient


def process_order(message) -> bool:
    """
    Simulate processing an order.
    Returns True if successful, False if processing failed.
    """
    order = json.loads(str(message))
    print(f"\n📦 Processing order: {order['order_id']}")
    print(f"   Customer : {order['customer']}")
    print(f"   Amount   : ₹{order['amount']}")
    print(f"   Items    : {', '.join(order['items'])}")

    # Simulate success — we will simulate failures later
    return True


def receive_orders():
    print("\n👂 Listening for orders...\n")

    client = ServiceBusClient.from_connection_string(CONNECTION_STRING)

    with client:
        receiver = client.get_queue_receiver(
            queue_name=QUEUE_NAME,
            max_wait_time=10    # stops after 10 seconds of no messages
        )

        with receiver:
            for message in receiver:

                # ── SYSTEM PROPERTIES ─────────────────
                print(f"📨 Message received")
                print(f"   sequence_number : {message.sequence_number}")
                print(f"   enqueued_time   : {message.enqueued_time_utc}")
                print(f"   delivery_count  : {message.delivery_count}")
                print(f"   expires_at      : {message.expires_at_utc}")

                # ── APPLICATION PROPERTIES ────────────
                props = message.application_properties
                print(f"   region          : {props.get(b'region', b'').decode()}")
                print(f"   priority        : {props.get(b'priority', b'').decode()}")
                print(f"   schema_version  : {props.get(b'schema_version', b'').decode()}")

                # ── PROCESS AND DECIDE OUTCOME ────────
                success = process_order(message)

                if success:
                    receiver.complete_message(message)
                    print(f"\n✅ Order completed and removed from queue")
                else:
                    receiver.abandon_message(message)
                    print(f"\n♻️  Order abandoned — back to queue")


if __name__ == "__main__":
    receive_orders()