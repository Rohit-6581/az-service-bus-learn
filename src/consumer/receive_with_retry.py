# src/consumer/receive_with_retry.py
#
# Purpose: Simulate real production failure handling
# What to expect:
#   Attempt 1 — fails validation → abandon → back to queue
#   Attempt 2 — fails validation → abandon → back to queue
#   Attempt 3 — fails validation → abandon → broker dead-letters automatically
#
# Key thing to observe: delivery_count increases each attempt

import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.config import CONNECTION_STRING, QUEUE_NAME

from azure.servicebus import ServiceBusClient


def validate_order(order: dict) -> tuple[bool, str]:
    """
    Validate order business rules.
    Returns (is_valid, reason_if_invalid)
    """
    if order.get("amount", 0) <= 0:
        return False, f"Invalid amount: {order.get('amount')}"

    if not order.get("items"):
        return False, "Order has no items"

    if not order.get("customer"):
        return False, "Missing customer name"

    return True, ""


def process_order_with_validation(message, receiver):
    """
    Process order with full validation and failure handling.
    Simulates real production consumer behaviour.
    """
    order = json.loads(str(message))

    print(f"\n{'='*45}")
    print(f"📨 Attempt #{message.delivery_count + 1} — order: {order['order_id']}")
    print(f"   delivery_count : {message.delivery_count}")
    print(f"   sequence_number: {message.sequence_number}")
    print(f"   message_id     : {message.message_id}")

    # Validate the order
    is_valid, reason = validate_order(order)

    if not is_valid:
        print(f"\n💥 Validation failed: {reason}")

        # Check if we have retries left
        # MaxDeliveryCount is 3 — abandon on first 2 failures
        # On 3rd failure broker will auto dead-letter after abandon
        print(f"   Abandoning message — broker will retry")
        print(f"   Retries remaining: {3 - (message.delivery_count + 1)}")
        receiver.abandon_message(message)
        return False

    # Valid order — process it
    print(f"\n✅ Order valid — processing")
    print(f"   Customer : {order['customer']}")
    print(f"   Amount   : ₹{order['amount']}")
    print(f"   Items    : {', '.join(order['items'])}")
    receiver.complete_message(message)
    return True


def receive_with_retry():
    print("\n👂 Listening for orders with validation...\n")
    print("Watch delivery_count increase with each failed attempt")
    print("After 3 failures broker moves message to Dead-letter Queue")
    print("=" * 45)

    client = ServiceBusClient.from_connection_string(CONNECTION_STRING)

    with client:
        receiver = client.get_queue_receiver(
            queue_name=QUEUE_NAME,
            max_wait_time=120    # wait longer to catch retries
        )

        with receiver:
            for message in receiver:
                process_order_with_validation(message, receiver)


if __name__ == "__main__":
    receive_with_retry()