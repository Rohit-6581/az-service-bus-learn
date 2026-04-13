# src/consumer/read_dlq.py
#
# Purpose: Read and inspect messages from the Dead-letter Queue
# The DLQ is automatically attached to every queue and subscription
# Nothing to create — it just exists
#
# Real production use case:
#   Engineers monitor DLQ depth
#   A growing DLQ means something is systematically broken
#   You inspect messages here to find the root cause

import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.config import CONNECTION_STRING, QUEUE_NAME

from azure.servicebus import ServiceBusClient, ServiceBusSubQueue


def read_dead_letter_queue():
    print("\n☠️  Reading Dead-letter Queue")
    print("=" * 45)
    print(f"Queue: {QUEUE_NAME}/$DeadLetterQueue")
    print("=" * 45)

    client = ServiceBusClient.from_connection_string(CONNECTION_STRING)

    with client:
        # ServiceBusSubQueue.DEAD_LETTER is the correct enum value
        # Everything else is identical to reading a normal queue
        dlq_receiver = client.get_queue_receiver(
            queue_name=QUEUE_NAME,
            sub_queue=ServiceBusSubQueue.DEAD_LETTER,
            max_wait_time=10
        )

        with dlq_receiver:
            messages_found = 0

            for message in dlq_receiver:
                messages_found += 1
                order = json.loads(str(message))

                print(f"\n📨 Dead-lettered Message #{messages_found}")
                print(f"   {'─'*40}")

                # ── WHY WAS IT DEAD-LETTERED ──────────
                print(f"   dead_letter_reason     : {message.dead_letter_reason}")
                print(f"   dead_letter_description: {message.dead_letter_error_description}")

                # ── HOW MANY TIMES WAS IT TRIED ───────
                print(f"   delivery_count         : {message.delivery_count}")
                print(f"   sequence_number        : {message.sequence_number}")
                print(f"   message_id             : {message.message_id}")
                print(f"   enqueued_time          : {message.enqueued_time_utc}")

                # ── WHAT WAS IN THE MESSAGE ───────────
                print(f"\n   📦 Message Body:")
                print(f"      order_id  : {order.get('order_id')}")
                print(f"      customer  : {order.get('customer')}")
                print(f"      amount    : {order.get('amount')}")
                print(f"      items     : {order.get('items')}")

                # ── APPLICATION PROPERTIES ────────────
                props = message.application_properties
                if props:
                    print(f"\n   📋 Application Properties:")
                    for key, value in props.items():
                        k = key.decode() if isinstance(key, bytes) else key
                        v = value.decode() if isinstance(value, bytes) else value
                        print(f"      {k}: {v}")

                # ── WHAT TO DO WITH IT ────────────────
                print(f"\n   🔧 Action: Completing (removing from DLQ)")
                print(f"      In production you would:")
                print(f"      1. Log this to your alerting system")
                print(f"      2. Fix the root cause")
                print(f"      3. Reprocess manually if needed")

                # Complete removes it from DLQ
                # In production you might just peek without completing
                # until you have fixed the root cause
                dlq_receiver.complete_message(message)
                print(f"   ✅ Removed from DLQ")

            if messages_found == 0:
                print("\n   Queue is empty — no dead-lettered messages found")
                print("   Run test_failure.py first to generate a poison message")
            else:
                print(f"\n{'='*45}")
                print(f"☠️  Total dead-lettered messages found: {messages_found}")
                print(f"{'='*45}")


if __name__ == "__main__":
    read_dead_letter_queue()