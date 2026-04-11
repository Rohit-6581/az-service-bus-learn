# src/config.py

CONNECTION_STRING = (
    "Endpoint=sb://localhost;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;"
    "UseDevelopmentEmulator=true;"
)

QUEUE_NAME = "orders-queue"
TOPIC_NAME = "orders-topic"

SUBSCRIPTIONS = {
    "warehouse": "warehouse",
    "email": "email",
    "analytics": "analytics"
}