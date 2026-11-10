"""
Kafka Producer

Publishes simulated customer events as JSON to Kafka.

Run:
    python -m src.kafka_producer
"""

import json
import sys
import time
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

import config

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    KAFKA_AVAILABLE = True

except ImportError as e:
    KAFKA_AVAILABLE = False
    print(f"[DEBUG] Kafka import failed: {e}")


class KafkaUnavailableError(Exception):
    """Raised when Kafka is unavailable."""


class ChurnEventProducer:

    def __init__(self, bootstrap_servers=None, topic=None):

        # Check kafka-python
        if not KAFKA_AVAILABLE:
            raise KafkaUnavailableError(
                "kafka-python is not installed. "
                "Run: pip install kafka-python"
            )

        # Get configuration
        self.topic = topic or config.KAFKA_TOPIC
        self.bootstrap_servers = (
            bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS
        )

        try:

            print(
                f"[INFO] Connecting to Kafka at "
                f"{self.bootstrap_servers}..."
            )

            self.producer = KafkaProducer(

                # Kafka broker
                bootstrap_servers=self.bootstrap_servers,

                # IMPORTANT:
                # Your Kafka broker is working with API version 3.9.0
                api_version=(3, 9, 0),

                # Convert Python dictionary → JSON
                value_serializer=lambda value:
                    json.dumps(
                        value,
                        default=str
                    ).encode("utf-8"),

                # Convert customer ID → bytes
                key_serializer=lambda key:
                    str(key).encode("utf-8")
                    if key is not None
                    else None,

                # Retry failed requests
                retries=3,

                # Connection timeout
                request_timeout_ms=10000,

                # Wait for broker acknowledgement
                acks="all"
            )

            print("[INFO] Kafka connection successful.")

        except Exception as e:

            raise KafkaUnavailableError(
                f"Could not connect to Kafka at "
                f"{self.bootstrap_servers}. "
                f"Error: {e}"
            ) from e


    def send_event(self, event: dict):

        """
        Send one customer event to Kafka.

        customer_id is used as the Kafka key so events
        belonging to the same customer stay ordered.
        """

        try:

            customer_id = event.get("customer_id")

            future = self.producer.send(
                self.topic,
                key=customer_id,
                value=event
            )

            # Wait for Kafka acknowledgement
            metadata = future.get(timeout=10)

            return True

        except KafkaError as e:

            raise KafkaUnavailableError(
                f"Failed to send event to Kafka: {e}"
            ) from e

        except Exception as e:

            raise KafkaUnavailableError(
                f"Unexpected Kafka error: {e}"
            ) from e


    def close(self):

        """Flush and close the Kafka producer."""

        try:

            self.producer.flush(timeout=5)
            self.producer.close(timeout=5)

            print("[INFO] Kafka producer closed.")

        except Exception:
            pass


def run_continuous_producer():

    """
    Generate simulated customer events continuously
    and publish them to Kafka.
    """

    from src.simulator import CustomerSimulator

    # Create simulator
    sim = CustomerSimulator()

    # Show population
    print(
        f"Population: {sim.population_summary()}"
    )

    # Connect to Kafka
    try:

        producer = ChurnEventProducer()

    except KafkaUnavailableError as e:

        print(f"[ERROR] {e}")
        sys.exit(1)

    print()
    print(
        f"Producing to Kafka topic "
        f"'{config.KAFKA_TOPIC}'"
    )

    print(
        f"Kafka broker: "
        f"{config.KAFKA_BOOTSTRAP_SERVERS}"
    )

    print()

    count = 0

    try:

        while (
            config.SIMULATION_MAX_EVENTS == 0
            or count < config.SIMULATION_MAX_EVENTS
        ):

            # Generate next customer event
            event = sim.next_event()

            try:

                # Send event
                producer.send_event(event)

                print(
                    f"Sent: "
                    f"{event['customer_id']} | "
                    f"tenure={event['tenure']} | "
                    f"support_calls={event['support_calls']} | "
                    f"data_usage={event['data_usage']}"
                )

                count += 1

            except KafkaUnavailableError as e:

                print(
                    f"[WARN] {e}"
                )

                print(
                    "[WARN] Retrying in 5 seconds..."
                )

                time.sleep(5)

                continue

            # Wait before generating next event
            time.sleep(
                config.SIMULATION_INTERVAL
            )

    except KeyboardInterrupt:

        print()
        print("Producer stopped by user.")

    finally:

        producer.close()


if __name__ == "__main__":

    run_continuous_producer()