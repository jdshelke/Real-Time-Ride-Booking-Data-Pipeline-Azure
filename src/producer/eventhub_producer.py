import json

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import DefaultAzureCredential

EVENT_HUB_FULLY_QUALIFIED_NAMESPACE = "uber-eventhub-ns.servicebus.windows.net"
EVENT_HUB_NAME = "uber-ride-events"

credential = DefaultAzureCredential()

producer = None

async def initialize_eventhub_producer():
    global producer

    # Create producer client
    producer = EventHubProducerClient(
        fully_qualified_namespace=EVENT_HUB_FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENT_HUB_NAME,
        credential=credential,
    )
    print("Event Hub Producer Started...")

async def close_eventhub_producer():
    global producer 

    if producer: 
        await producer.close() 
        
    await credential.close() 
    print("Event Hub Producer Closed")

async def send_events_to_eventhub(events_data_list):

    global producer
    # Create batch
    event_data_batch = await producer.create_batch()

    batch_count = 0

    # Read events from events_data_list
    for event in events_data_list:
        event_json = json.dumps(event, default=str)

        try:
            event_data_batch.add(EventData(event_json))
            batch_count += 1
        except ValueError:
            # Send Current Full Batch
            await producer.send_batch(event_data_batch)

            # Create new batch
            event_data_batch = await producer.create_batch()

            # Add current event to new batch 
            event_data_batch.add(EventData(event_json))
            batch_count += 1


    # Send batch to Event Hub
    if len(event_data_batch) > 0:
        await producer.send_batch(event_data_batch)

    print(f"Sent {batch_count} events to Event Hub")

