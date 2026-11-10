"""
Kafka Consumer

Reads customer events from Kafka, validates them, runs them through the
ETL pipeline:

    Customer Event
          ↓
    Feature Engineering
          ↓
    Churn Prediction
          ↓
    Customer Health Score
          ↓
    Alerting
          ↓
    SQLite Database

Run:
    python -m src.kafka_consumer
"""

import json
import sys
import time
from pathlib import Path


# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# PROJECT IMPORTS
# ============================================================

import config
from src import database
from src.etl_pipeline import ETLError, process_event


# ============================================================
# KAFKA IMPORT
# ============================================================

try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError

    KAFKA_AVAILABLE = True
    KAFKA_IMPORT_ERROR = None

except ImportError as e:
    KAFKA_AVAILABLE = False
    KAFKA_IMPORT_ERROR = str(e)


# ============================================================
# CUSTOM EXCEPTION
# ============================================================

class KafkaUnavailableError(Exception):
    """Raised when Kafka is unavailable."""


# ============================================================
# BUILD KAFKA CONSUMER
# ============================================================

def build_consumer(
    topic=None,
    bootstrap_servers=None,
    group_id=None
):
    """
    Create and return a Kafka consumer.
    """

    # --------------------------------------------------------
    # Check Kafka Python package
    # --------------------------------------------------------

    if not KAFKA_AVAILABLE:

        raise KafkaUnavailableError(
            "kafka-python could not be imported.\n"
            f"Import error: {KAFKA_IMPORT_ERROR}\n\n"
            "Install it using:\n"
            "python -m pip install kafka-python"
        )

    # --------------------------------------------------------
    # Get configuration
    # --------------------------------------------------------

    kafka_topic = (
        topic
        or config.KAFKA_TOPIC
    )

    kafka_servers = (
        bootstrap_servers
        or config.KAFKA_BOOTSTRAP_SERVERS
    )

    kafka_group = (
        group_id
        or config.KAFKA_CONSUMER_GROUP
    )

    print("[INFO] Creating Kafka consumer...")
    print(f"[INFO] Broker : {kafka_servers}")
    print(f"[INFO] Topic  : {kafka_topic}")
    print(f"[INFO] Group  : {kafka_group}")

    # --------------------------------------------------------
    # Create Kafka consumer
    # --------------------------------------------------------

    try:

        consumer = KafkaConsumer(

            kafka_topic,

            bootstrap_servers=kafka_servers,

            group_id=kafka_group,

            value_deserializer=lambda value:
                json.loads(
                    value.decode("utf-8")
                ),

            key_deserializer=lambda key:
                key.decode("utf-8")
                if key
                else None,

            auto_offset_reset="latest",

            enable_auto_commit=True,

            # Allows the loop to wake up periodically
            consumer_timeout_ms=1000,

            # Network timeout
            request_timeout_ms=30000,

            # Consumer session timeout
            session_timeout_ms=10000,
        )

        print(
            "[SUCCESS] Kafka consumer connected."
        )

        return consumer

    except Exception as e:

        raise KafkaUnavailableError(
            f"Could not connect to Kafka at "
            f"{kafka_servers}.\n"
            f"Error: {e}\n\n"
            "Make sure the Kafka broker is running "
            "on port 9092."
        ) from e


# ============================================================
# PROCESS ONE KAFKA MESSAGE
# ============================================================

def process_message(message):
    """
    Process one Kafka message through the ETL pipeline.
    """

    raw_event = message.value

    try:

        result = process_event(raw_event)

        return result

    except ETLError as e:

        print(
            f"[SKIP] Invalid/unprocessable event: {e}"
        )

        return None

    except Exception as e:

        print(
            f"[ERROR] Unexpected processing failure: {e}"
        )

        return None


# ============================================================
# DISPLAY PROCESSED RESULT
# ============================================================

def display_result(result):
    """
    Display customer churn prediction,
    health score and alert information.
    """

    if not result:
        return

    try:

        # ----------------------------------------------------
        # Customer ID
        # ----------------------------------------------------

        customer_id = (
            result["engineered_row"]
            ["customer_id"]
        )

        # ----------------------------------------------------
        # Churn Probability
        # ----------------------------------------------------

        churn_probability = (
            result["prediction"]
            ["churn_probability"]
        )

        # ----------------------------------------------------
        # Health Score
        # ----------------------------------------------------

        health_score = (
            result["health"]
            ["health_score"]
        )

        # ----------------------------------------------------
        # Alert
        # ----------------------------------------------------

        alert = result.get("alert")

        alert_note = ""

        if alert:

            risk_level = alert.get(
                "risk_level",
                "UNKNOWN"
            )

            alert_note = (
                f"  ALERT[{risk_level}]"
            )

        # ----------------------------------------------------
        # Print result
        # ----------------------------------------------------

        print(
            f"Processed {customer_id}: "
            f"churn={churn_probability:.2f} "
            f"health={health_score}"
            f"{alert_note}"
        )

    except KeyError as e:

        print(
            f"[WARN] Missing result field: {e}"
        )

    except Exception as e:

        print(
            f"[WARN] Could not display result: {e}"
        )


# ============================================================
# CONSUMER LOOP
# ============================================================

def run_consumer_loop():
    """
    Continuously consume customer events from Kafka.
    """

    # --------------------------------------------------------
    # Header
    # --------------------------------------------------------

    print()
    print("=" * 60)
    print("CUSTOMER CHURN - KAFKA CONSUMER")
    print("=" * 60)
    print()

    # --------------------------------------------------------
    # Initialize database
    # --------------------------------------------------------

    try:

        database.init_db()

        print(
            "[SUCCESS] Database initialized."
        )

    except Exception as e:

        print(
            f"[ERROR] Database initialization failed: {e}"
        )

        sys.exit(1)

    # --------------------------------------------------------
    # Create Kafka consumer
    # --------------------------------------------------------

    try:

        consumer = build_consumer()

    except KafkaUnavailableError as e:

        print()
        print("[ERROR] Kafka unavailable.")
        print(e)
        print()

        sys.exit(1)

    # --------------------------------------------------------
    # Consumer information
    # --------------------------------------------------------

    print()

    print(
        f"Listening on Kafka topic "
        f"'{config.KAFKA_TOPIC}'"
    )

    print(
        f"Kafka broker: "
        f"{config.KAFKA_BOOTSTRAP_SERVERS}"
    )

    print()

    print(
        "Waiting for customer events..."
    )

    print(
        "Press CTRL+C to stop."
    )

    print("-" * 60)

    # --------------------------------------------------------
    # Counters
    # --------------------------------------------------------

    processed = 0
    errors = 0

    # ========================================================
    # MAIN LOOP
    # ========================================================

    try:

        while True:

            try:

                received_message = False

                # ------------------------------------------------
                # Read messages from Kafka
                # ------------------------------------------------

                for message in consumer:

                    received_message = True

                    print(
                        f"[RECEIVED] "
                        f"partition={message.partition} "
                        f"offset={message.offset}"
                    )

                    # --------------------------------------------
                    # Process event
                    # --------------------------------------------

                    result = process_message(
                        message
                    )

                    # --------------------------------------------
                    # Successful processing
                    # --------------------------------------------

                    if result is not None:

                        processed += 1

                        # Display result
                        display_result(
                            result
                        )

                        # ----------------------------------------
                        # Save processing metric
                        # ----------------------------------------

                        try:

                            database.record_metric(
                                "events_processed_total",
                                processed
                            )

                        except Exception as e:

                            print(
                                "[WARN] Could not record "
                                f"metric: {e}"
                            )

                    # --------------------------------------------
                    # Failed processing
                    # --------------------------------------------

                    else:

                        errors += 1

                # ------------------------------------------------
                # No message received
                # ------------------------------------------------

                if not received_message:

                    time.sleep(0.2)

            # ----------------------------------------------------
            # Kafka error
            # ----------------------------------------------------

            except KafkaError as e:

                errors += 1

                print(
                    f"[WARN] Kafka error: {e}"
                )

                print(
                    "[INFO] Retrying in 5 seconds..."
                )

                time.sleep(5)

            # ----------------------------------------------------
            # Unexpected error
            # ----------------------------------------------------

            except Exception as e:

                errors += 1

                print(
                    f"[ERROR] Unexpected "
                    f"consumer error: {e}"
                )

                print(
                    "[INFO] Retrying in 5 seconds..."
                )

                time.sleep(5)

    # ========================================================
    # CTRL + C
    # ========================================================

    except KeyboardInterrupt:

        print()
        print("=" * 60)
        print("CONSUMER STOPPED")
        print("=" * 60)

        print(
            f"Events processed : {processed}"
        )

        print(
            f"Errors           : {errors}"
        )

    # ========================================================
    # CLEANUP
    # ========================================================

    finally:

        try:

            consumer.close()

            print(
                "[INFO] Kafka consumer closed."
            )

        except Exception:

            pass


# ============================================================
# PROGRAM ENTRY POINT
# ============================================================

if __name__ == "__main__":

    run_consumer_loop()