from azure.servicebus import ServiceBusClient

CONNECTION_STRING = (
    "Endpoint=sb://localhost;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;"
    "UseDevelopmentEmulator=true;"
)

try:
    client = ServiceBusClient.from_connection_string(CONNECTION_STRING)
    print("✅ Connected to Service Bus Emulator successfully!")
    client.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")