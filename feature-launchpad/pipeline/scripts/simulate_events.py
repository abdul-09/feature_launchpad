"""
Event Data Simulator

Generates realistic user event data for testing the pipeline.
Simulates user behavior patterns including:
- Session-based interactions
- Realistic completion rates
- Time-based patterns
- Device distribution
- Drop-off behavior

Usage:
    python simulate_events.py --users 1000 --days 30 --output ./data
    python simulate_events.py --kafka --users 100  # Stream to Kafka
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Generator, Optional
import os

# Try to import Kafka, but make it optional
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


# Configuration
DEVICE_DISTRIBUTION = {
    "desktop": 0.55,
    "mobile": 0.35,
    "tablet": 0.10,
}

TIMEZONES = [
    "America/New_York",
    "America/Los_Angeles", 
    "America/Chicago",
    "Europe/London",
    "Europe/Paris",
    "Asia/Tokyo",
    "Asia/Singapore",
]

USE_CASES = ["analytics", "engineering", "ml", "realtime"]

FEATURES = [
    ("opensource", 0.6),
    ("cloud", 0.5),
    ("sql", 0.7),
    ("python", 0.65),
    ("governance", 0.3),
    ("lowcode", 0.25),
]

RECOMMENDATIONS = [
    {"id": "databricks", "name": "Databricks"},
    {"id": "snowflake", "name": "Snowflake"},
    {"id": "duckdb", "name": "DuckDB"},
    {"id": "kafka", "name": "Apache Kafka"},
    {"id": "airflow", "name": "Apache Airflow"},
    {"id": "metabase", "name": "Metabase"},
]

SHARE_PLATFORMS = ["twitter", "linkedin", "email", "copy_link"]


class EventSimulator:
    """Simulates realistic user event streams."""
    
    def __init__(self, seed: Optional[int] = None):
        if seed:
            random.seed(seed)
        self.user_pool = []
        
    def generate_user_id(self) -> str:
        """Generate a unique anonymous user ID."""
        return f"anon_{uuid.uuid4().hex[:16]}"
    
    def generate_session_id(self) -> str:
        """Generate a session ID."""
        return str(uuid.uuid4())
    
    def pick_device(self) -> str:
        """Pick a device type based on distribution."""
        r = random.random()
        cumulative = 0
        for device, prob in DEVICE_DISTRIBUTION.items():
            cumulative += prob
            if r <= cumulative:
                return device
        return "desktop"
    
    def generate_context(self, device: str) -> dict:
        """Generate event context."""
        screen_sizes = {
            "desktop": [(1920, 1080), (1440, 900), (2560, 1440)],
            "mobile": [(375, 812), (414, 896), (390, 844)],
            "tablet": [(768, 1024), (834, 1194), (820, 1180)],
        }
        
        screen = random.choice(screen_sizes.get(device, [(1920, 1080)]))
        
        return {
            "page_url": "https://feature-launchpad.demo/configurator",
            "page_path": "/configurator",
            "referrer": random.choice([
                "https://google.com",
                "https://twitter.com",
                "https://linkedin.com",
                "",
                "direct",
            ]),
            "device_type": device,
            "screen_width": screen[0],
            "screen_height": screen[1],
            "viewport_width": int(screen[0] * 0.95),
            "viewport_height": int(screen[1] * 0.85),
            "timezone": random.choice(TIMEZONES),
            "locale": random.choice(["en-US", "en-GB", "de-DE", "fr-FR", "ja-JP"]),
        }
    
    def simulate_session(
        self, 
        user_id: str, 
        session_start: datetime,
        completion_probability: float = 0.65
    ) -> Generator[dict, None, None]:
        """
        Simulate a complete user session.
        
        Generates events for:
        1. Session start
        2. Quiz start
        3. Each question (with interactions)
        4. Completion (probabilistic)
        5. Result viewing and sharing (probabilistic)
        6. Session end
        """
        session_id = self.generate_session_id()
        device = self.pick_device()
        context = self.generate_context(device)
        current_time = session_start
        
        total_steps = 4
        
        # Helper to create event
        def make_event(event_type: str, properties: dict = None) -> dict:
            nonlocal current_time
            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "session_id": session_id,
                "event_type": event_type,
                "event_properties": properties or {},
                "feature_name": "product_configurator",
                "context": context,
                "timestamp": current_time.isoformat() + "Z",
            }
            # Advance time
            current_time += timedelta(milliseconds=random.randint(500, 3000))
            return event
        
        # Session start
        yield make_event("session_start")
        
        # Quiz start
        yield make_event("quiz_started", {"total_steps": total_steps})
        
        # Question 1: Team size slider
        yield make_event("question_viewed", {
            "question_id": 1, 
            "step_number": 1,
            "total_steps": total_steps
        })
        
        team_size = random.randint(1, 100)
        prev_value = 10
        for _ in range(random.randint(1, 4)):
            current_time += timedelta(milliseconds=random.randint(800, 2000))
            new_value = random.randint(1, 100)
            yield make_event("slider_adjusted", {
                "question_id": 1,
                "slider_value": new_value,
                "previous_value": prev_value,
            })
            prev_value = new_value
            team_size = new_value
        
        yield make_event("quiz_step_completed", {
            "step_number": 1,
            "total_steps": total_steps,
            "time_on_step_ms": random.randint(5000, 15000),
        })
        
        # Check for early drop-off
        if random.random() > 0.9:  # 10% drop at step 1
            yield make_event("quiz_abandoned", {"step_number": 1})
            yield make_event("session_end", {"total_time_ms": random.randint(10000, 30000)})
            return
        
        # Question 2: Use case selection
        yield make_event("question_viewed", {
            "question_id": 2,
            "step_number": 2,
            "total_steps": total_steps
        })
        
        use_case = random.choice(USE_CASES)
        yield make_event("option_selected", {
            "question_id": 2,
            "option_id": use_case,
            "option_label": use_case.replace("_", " ").title(),
        })
        
        yield make_event("quiz_step_completed", {
            "step_number": 2,
            "total_steps": total_steps,
            "time_on_step_ms": random.randint(3000, 10000),
        })
        
        # Check for drop-off
        if random.random() > 0.85:  # 15% drop at step 2
            yield make_event("quiz_abandoned", {"step_number": 2})
            yield make_event("session_end", {"total_time_ms": random.randint(20000, 45000)})
            return
        
        # Question 3: Feature selection (multi)
        yield make_event("question_viewed", {
            "question_id": 3,
            "step_number": 3,
            "total_steps": total_steps
        })
        
        selected_features = []
        for feature_id, prob in FEATURES:
            if random.random() < prob:
                selected_features.append(feature_id)
                yield make_event("option_selected", {
                    "question_id": 3,
                    "option_id": feature_id,
                    "option_label": feature_id.replace("_", " ").title(),
                })
        
        yield make_event("quiz_step_completed", {
            "step_number": 3,
            "total_steps": total_steps,
            "time_on_step_ms": random.randint(5000, 20000),
            "selected_options": selected_features,
        })
        
        # Check for drop-off
        if random.random() > completion_probability:
            yield make_event("quiz_abandoned", {"step_number": 3})
            yield make_event("session_end", {"total_time_ms": random.randint(40000, 60000)})
            return
        
        # Question 4: Budget slider
        yield make_event("question_viewed", {
            "question_id": 4,
            "step_number": 4,
            "total_steps": total_steps
        })
        
        budget = random.randint(0, 500)
        prev_budget = 100
        for _ in range(random.randint(1, 3)):
            current_time += timedelta(milliseconds=random.randint(500, 1500))
            new_budget = random.randint(0, 500)
            yield make_event("slider_adjusted", {
                "question_id": 4,
                "slider_value": new_budget,
                "previous_value": prev_budget,
            })
            prev_budget = new_budget
            budget = new_budget
        
        yield make_event("quiz_step_completed", {
            "step_number": 4,
            "total_steps": total_steps,
            "time_on_step_ms": random.randint(3000, 8000),
        })
        
        # Quiz completed!
        recommendation = random.choice(RECOMMENDATIONS)
        match_score = random.randint(65, 99)
        total_time = random.randint(45000, 120000)
        
        yield make_event("quiz_completed", {
            "recommendation_id": recommendation["id"],
            "recommendation_name": recommendation["name"],
            "match_score": match_score,
            "total_time_ms": total_time,
        })
        
        # Result viewed
        yield make_event("result_viewed", {
            "recommendation_id": recommendation["id"],
            "recommendation_name": recommendation["name"],
            "match_score": match_score,
        })
        
        # Maybe share (higher for high match scores)
        share_prob = 0.1 + (match_score / 100) * 0.2
        if random.random() < share_prob:
            platform = random.choice(SHARE_PLATFORMS)
            yield make_event("result_shared", {
                "recommendation_id": recommendation["id"],
                "share_platform": platform,
            })
        
        # Maybe click CTA
        if random.random() < 0.3:
            yield make_event("cta_clicked", {
                "cta_id": "learn_more",
                "cta_label": "Learn More",
            })
        
        # Session end
        yield make_event("session_end", {"total_time_ms": total_time})
    
    def simulate_day(
        self,
        date: datetime,
        num_users: int,
        returning_users: list = None,
    ) -> Generator[dict, None, None]:
        """Simulate all events for a single day."""
        
        # Mix of new and returning users
        if returning_users:
            num_returning = min(len(returning_users), int(num_users * 0.3))
            users = random.sample(returning_users, num_returning)
            users += [self.generate_user_id() for _ in range(num_users - num_returning)]
        else:
            users = [self.generate_user_id() for _ in range(num_users)]
        
        # Distribute sessions throughout the day
        for user_id in users:
            # Random time during the day (weighted toward business hours)
            hour = random.choices(
                range(24),
                weights=[1,1,1,1,1,2,3,5,7,8,9,9,8,8,8,7,6,5,4,3,3,2,2,1]
            )[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            
            session_start = date.replace(hour=hour, minute=minute, second=second)
            
            # Returning users have higher completion rates
            completion_prob = 0.75 if user_id in (returning_users or []) else 0.60
            
            for event in self.simulate_session(user_id, session_start, completion_prob):
                yield event
        
        # Return the user list for tracking returners
        return users


def write_to_files(events: list, output_dir: str, date: datetime):
    """Write events to Parquet-like JSON files (for simplicity)."""
    os.makedirs(output_dir, exist_ok=True)
    
    date_str = date.strftime("%Y-%m-%d")
    filepath = os.path.join(output_dir, f"events_{date_str}.json")
    
    with open(filepath, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    
    print(f"Wrote {len(events)} events to {filepath}")


def stream_to_kafka(events: list, bootstrap_servers: str, topic: str):
    """Stream events to Kafka."""
    if not KAFKA_AVAILABLE:
        print("Kafka not available. Install kafka-python: pip install kafka-python")
        return
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    
    for event in events:
        producer.send(topic, key=event["user_id"], value=event)
    
    producer.flush()
    print(f"Streamed {len(events)} events to Kafka topic '{topic}'")


def main():
    parser = argparse.ArgumentParser(description="Simulate user events")
    parser.add_argument("--users", type=int, default=100, help="Users per day")
    parser.add_argument("--days", type=int, default=7, help="Number of days to simulate")
    parser.add_argument("--output", type=str, default="./data/simulated", help="Output directory")
    parser.add_argument("--kafka", action="store_true", help="Stream to Kafka instead of files")
    parser.add_argument("--kafka-servers", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", type=str, default="user_events", help="Kafka topic")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    simulator = EventSimulator(seed=args.seed)
    
    # Track returning users
    all_users = []
    
    # Simulate each day
    end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    
    for day_offset in range(args.days - 1, -1, -1):
        current_date = end_date - timedelta(days=day_offset)
        print(f"\nSimulating {current_date.strftime('%Y-%m-%d')}...")
        
        # Collect all events for the day
        day_events = []
        day_users = []
        
        for event in simulator.simulate_day(current_date, args.users, all_users):
            day_events.append(event)
            if event["event_type"] == "session_start":
                day_users.append(event["user_id"])
        
        # Track users for returning user simulation
        all_users.extend([u for u in day_users if u not in all_users])
        # Keep only recent users for returning pool
        all_users = all_users[-args.users * 3:]
        
        # Output events
        if args.kafka:
            stream_to_kafka(day_events, args.kafka_servers, args.kafka_topic)
        else:
            write_to_files(day_events, args.output, current_date)
        
        print(f"  Generated {len(day_events)} events from {len(set(day_users))} users")
    
    print("\n✓ Simulation complete!")


if __name__ == "__main__":
    main()
