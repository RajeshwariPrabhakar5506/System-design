import asyncio
import json
import uuid
from sqlalchemy import create_engine, Column, String, Float
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, sessionmaker

from aiokafka import AIOKafkaConsumer
from ingestion import Event
from irrigation import IrrigationService

# --- DATABASE SETUP (Move this outside the class) ---
# Note: Ensure the host 'db_service' and port '5632' match your docker-compose exactly
DATABASE_URL = "postgresql://agriadmin:agriadmin123@db_service:5432/agri_db"

Base = declarative_base()
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class EventModel(Base):
    __tablename__ = "events"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sensor_id = Column(String(100), nullable=False)
    temperature = Column(Float, nullable=False)

# This is the line that actually creates the table in Postgres
def init_db():
    print("Initializing database...")
    Base.metadata.create_all(bind=engine)

# --- CONSUMER UTILITY ---

class KafkaEventConsumer:
    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            "sensor.events",
            bootstrap_servers=self.bootstrap_servers,
            group_id="sensor_data_group",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
        )
        await self.consumer.start()

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()

    def store_event(self, event: Event):
        # Open a session, add the record, and commit
        with SessionLocal() as session:
            db_event = EventModel(
                sensor_id=event.sensor_id, 
                temperature=event.temperature
            )
            session.add(db_event)
            session.commit()
            print(f"✅ Stored in DB: {event.sensor_id} → {event.temperature}", flush=True)

    async def process_events(self):
        await self.start()
        try:
            print("Listening for Kafka messages...")
            async for msg in self.consumer:
                event = Event(**msg.value)
                
                # 1. Store in Database (Crucial step!)
                self.store_event(event)
                
                # 2. Trigger Irrigation Service
                irrigation = IrrigationService()
                irrigation.create_irrigation_data(event)
        finally:
            await self.stop()

if __name__ == "__main__":
    # Ensure tables are created BEFORE starting the async loop
    init_db()
    
    # Run the consumer
    consumer = KafkaEventConsumer()
    try:
        asyncio.run(consumer.process_events())
    except KeyboardInterrupt:
        pass