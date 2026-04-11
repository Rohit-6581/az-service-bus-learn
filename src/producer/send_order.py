# src/producer/send_order.py

import json
import sys
import os
from datetime import timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.config import CONNECTION_STRING, QUEUE_NAME

from azure.servicebus import ServiceBusClient, ServiceBusMessage


def build_order_message(order: dict) -> ServiceBusMessage:
    """
    Build a fully annotated Service Bus message from an order dict.
    Every property maps to something we covered in Concept 4.
    """
    return ServiceBusMessage(
        # ── BODY ──────────────────────────────────────
        body=json.dumps(order),

        # ── IDENTITY ──────────────────────────────────
        message_id=f"order-{order['order_id']}",  # duplicate detection key
        correlation_id=f"customer-{order['customer_id']}",  # tracing

        # ── LABEL ─────────────────────────────────────
        subject="new-order",

        # ── EXPIRY ────────────────────────────────────
        time_to_live=timedelta(hours=24),

        # ── CUSTOM ENVELOPE ───────────────────────────
        application_properties={
            "region": order.get("region", "unknown"),
            "priority": "high" if order["amount"] > 500 else "normal",
            "order_type": order.get("order_type", "food"),
            "schema_version": "1.0"
        }
    )


def send_order(order: dict):
    print(f"\n📤 Sending order: {order['order_id']}")

    client = ServiceBusClient.from_connection_string(CONNECTION_STRING)

    with client:
        sender = client.get_queue_sender(queue_name=QUEUE_NAME)

        with sender:
            message = build_order_message(order)
            sender.send_messages(message)
            print(f"✅ Sent successfully")
            print(f"   message_id  : {message.message_id}")
            print(f"   subject     : {message.subject}")
            print(f"   priority    : {message.application_properties['priority']}")
            print(f"   region      : {message.application_properties['region']}")


if __name__ == "__main__":
    ## single test order
    # order = {
    #     "order_id": "1234",
    #     "customer_id": "cust-99",
    #     "customer": "Rahul",
    #     "amount": 850,
    #     "region": "jharkhand",
    #     "order_type": "food",
    #     "items": ["burger", "fries"]
    # }
    # send_order(order)

    # Multiple Orders
    orders = [
    {
        "order_id": "1235",
        "customer_id": "cust-100",
        "customer": "Priya",
        "amount": 250,
        "region": "jharkhand",
        "order_type": "food",
        "items": ["pizza"]
    },
    {
        "order_id": "1236",
        "customer_id": "cust-101",
        "customer": "Amit",
        "amount": 1200,
        "region": "delhi",
        "order_type": "food",
        "items": ["biryani", "raita", "dessert"]
    },
    {
        "order_id": "1237",
        "customer_id": "cust-102",
        "customer": "Sara",
        "amount": 450,
        "region": "jharkhand",
        "order_type": "food",
        "items": ["wrap", "juice"]
    }
    ]

    for order in orders:
        send_order(order)


    