import random
import time
from datetime import datetime
from kafka import KafkaProducer #guys, you have to do pip install kafka in the terminal before running this
 
# Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "funding_event"
EVENTS_PER_SECOND = 2
RUN_DURATION_SECONDS = 300

# Sectors (matching investor preferences)
SECTORS = [
    "AI", "SaaS", "Fintech", "HealthTech", "E-commerce", 
    "CleanTech", "Web3", "IoT", "Gaming", "Mobility"
]

# Funding stages
STAGES = ["Seed", "Series A", "Series B", "Series C"]

# Locations
LOCATIONS = [
    "San Francisco", "New York", "London", "Berlin", "Singapore", 
    "Bangalore", "Tel Aviv", "Toronto", "Boston", "Palo Alto",
    "Los Angeles", "Seattle", "Austin", "Chicago", "Miami",
    "Paris", "Amsterdam", "Stockholm", "Dublin", "Barcelona"
]

# Startup name components
PREFIXES = [
    "Smart", "Neuro", "Data", "Cloud", "Quantum", "Cyber", "Robo", 
    "Eco", "Bio", "Nano", "Meta", "Hyper", "Auto", "Digi", "Tech",
    "Rapid", "Next", "Future", "Alpha", "Beta", "Prime", "Core"
]

SUFFIXES = [
    "AI", "Tech", "Labs", "Systems", "Solutions", "Analytics", 
    "Platform", "Network", "Hub", "Dynamics", "Innovations", "Works",
    "Stream", "Flow", "Pulse", "Sync", "Nexus", "Vertex", "Matrix"
]

def generate_event(event_id):
    """Generate a single startup funding event"""
    
    # Basic info
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    startup_id = f"STR_{event_id:05d}"
    startup_name = f"{random.choice(PREFIXES)}{random.choice(SUFFIXES)}"
    sector = random.choice(SECTORS)
    stage = random.choice(STAGES)
    location = random.choice(LOCATIONS)
    
    # Financial metrics based on stage
    stage_config = {
        "Seed": (300000, 2000000, 3, 15),
        "Series A": (2000000, 15000000, 10, 50),
        "Series B": (10000000, 50000000, 30, 150),
        "Series C": (30000000, 150000000, 80, 300)
    }
    
    min_f, max_f, min_t, max_t = stage_config[stage]
    funding_amount = random.randint(min_f, max_f)
    team_size = random.randint(min_t, max_t)
    
    # Additional metrics
    months_since_founded = random.randint(6, 60)
    revenue = 0 if stage == "Seed" else random.randint(0, funding_amount // 2)
    mrr = revenue // 12 if revenue > 0 else 0
    team_growth_rate = round(random.uniform(0.05, 0.4), 2)
    
    # Event type
    event_types = ["funding_round", "team_expansion", "product_launch"]
    event_type = random.choice(event_types)
    
    # Create event line (similar to truck_geo_event format)
    event_line = (
        f"{timestamp}|startup_event|{event_id}|{startup_id}|{startup_name}|"
        f"{sector}|{stage}|{funding_amount}|{location}|{team_size}|"
        f"{months_since_founded}|{revenue}|{mrr}|{team_growth_rate}|{event_type}"
    )
    
    return event_line

def run_generator():
    """Run continuous event generator producing to Kafka"""
    
    print(f"Starting Startup Event Generator (Kafka Producer)...")
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Events per second: {EVENTS_PER_SECOND}")
    print(f"Duration: {RUN_DURATION_SECONDS} seconds")
    print("-" * 80)
    
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: v.encode('utf-8'),
        acks='all',
        retries=3,
        max_block_ms=10000
    )
    
    event_id = 1
    start_time = time.time()
    
    try:
        while (time.time() - start_time) < RUN_DURATION_SECONDS:
            # Generate event
            event = generate_event(event_id)
            
            # Send to Kafka
            future = producer.send(KAFKA_TOPIC, value=event)
            record_metadata = future.get(timeout=10)
            
            # Print progress
            if event_id % 10 == 0:
                elapsed = int(time.time() - start_time)
                print(f"[{elapsed}s] Sent {event_id} events | "
                      f"Partition: {record_metadata.partition} | "
                      f"Offset: {record_metadata.offset}")
            
            # Sleep to control rate
            time.sleep(1.0 / EVENTS_PER_SECOND)
            
            event_id += 1
            
    except KeyboardInterrupt:
        print("\n⚠️  Generator interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        producer.flush()
        producer.close()
        print("-" * 80)
        print(f"✅ Generator finished!")
        print(f"   Total events: {event_id - 1}")
        print(f"   Total time: {int(time.time() - start_time)} seconds")

if __name__ == "__main__":
    run_generator()
