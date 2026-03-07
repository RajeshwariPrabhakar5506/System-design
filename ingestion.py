"""
Kafka Producer Utility
"""

import asyncio
import json

from pydantic import BaseModel
from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "sensor.events"

class Event(BaseModel):
    sensor_id: str
    temperature: float

class KafkaEventProducer:
    
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    async def start(self):
        if self.producer is None:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Changed key_serializer to handle potential None/string logic
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k
            )
            await self.producer.start()

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            self.producer = None

    async def send_event(self, event: Event, topic_name: str = TOPIC_NAME):
        event_data = event.model_dump()
        # Fixed: changed 'topic' to 'topic_name' to match argument
        result = await self.producer.send_and_wait(
            topic=topic_name, 
            value=event_data, 
            key=event.sensor_id
        )
        return {
            "topic": result.topic,
            "partition": result.partition,
            "offset": result.offset
        }

    async def publish_event(self, event: Event):
        await self.start()
        try:
            result = await self.send_event(event)
            print(result)
            # Fixed: removed the dot after await
            await asyncio.sleep(1) 
        finally:
            await self.stop()