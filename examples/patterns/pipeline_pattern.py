"""
Pipeline Pattern for StreamMachine

This example demonstrates a chain of agents processing messages:
- Producer → Raw Processing → Transformation → Enrichment → Output
- Each stage processes and forwards to next stream
- Error handling at each stage

Run with: python pipeline_pattern.py
"""
import asyncio
import json
from streammachine import App, Message

app = App(name="pipeline_example", to_scan=True)


# =============================================================================
# Stage 1: Producer
# =============================================================================

@app.timer(2)
async def raw_data_producer():
    """Produce raw data to start the pipeline."""
    import random

    data = {
        "id": f"msg_{asyncio.get_event_loop().time():.0f}",
        "value": random.randint(1, 100),
        "source": "producer",
        "timestamp": asyncio.get_event_loop().time(),
    }

    await app.send("raw_data", data)
    print(f"[Producer] Sent raw data: {data['id']}")


# =============================================================================
# Stage 2: Raw Processing
# =============================================================================

@app.agent("raw_data", group="processors")
async def raw_processor(record: Message):
    """Process raw data and validate."""
    msg = record.message

    # Validation
    if not msg.get("id"):
        print("[RawProcessor] Invalid message: missing id")
        await app.send("errors", {"error": "missing_id", "data": str(msg)})
        return

    if not msg.get("value"):
        print("[RawProcessor] Invalid message: missing value")
        await app.send("errors", {"error": "missing_value", "data": str(msg)})
        return

    # Processing
    processed = {
        "id": msg["id"],
        "value": int(msg["value"]),
        "source": msg.get("source", "unknown"),
        "validated": "true",
        "processed_at": asyncio.get_event_loop().time(),
    }

    print(f"[RawProcessor] Validated: {processed['id']}")
    await app.send("validated_data", processed)


# =============================================================================
# Stage 3: Transformation
# =============================================================================

@app.agent("validated_data", group="transformers")
async def transformer(record: Message):
    """Transform validated data."""
    msg = record.message

    # Apply transformations
    transformed = {
        **msg,
        # Calculate derived values
        "value_squared": int(msg["value"]) ** 2,
        "value_category": "high" if int(msg["value"]) > 50 else "low",
        # Add metadata
        "transformed_at": asyncio.get_event_loop().time(),
    }

    print(f"[Transformer] Transformed: {transformed['id']}")
    await app.send("transformed_data", transformed)


# =============================================================================
# Stage 4: Enrichment
# =============================================================================

@app.agent("transformed_data", group="enrichers")
async def enricher(record: Message):
    """Enrich transformed data with external data."""
    msg = record.message

    # Simulate enriching with external data
    # In production, this might call an API or query a database
    enrichment_data = {
        "user_segment": "premium" if int(msg["value"]) > 70 else "standard",
        "region": "us-east",
        "enriched_at": asyncio.get_event_loop().time(),
    }

    enriched = {
        **msg,
        **enrichment_data,
    }

    print(f"[Enricher] Enriched: {enriched['id']}")
    await app.send("enriched_data", enriched)


# =============================================================================
# Stage 5: Final Output
# =============================================================================

@app.agent("enriched_data", group="output_handlers")
async def final_output(record: Message):
    """Handle final enriched output."""
    msg = record.message

    # Calculate final metrics
    total_time = msg.get("enriched_at", 0) - msg.get("timestamp", 0)

    output = {
        **msg,
        "pipeline_latency_ms": total_time * 1000,
        "pipeline_complete": "true",
    }

    print(f"[Output] Final output: {output['id']}")
    print(f"  Value: {output['value']} → {output['value_squared']} ({output['value_category']})")
    print(f"  Latency: {output['pipeline_latency_ms']:.2f}ms")

    # Send to final output stream
    await app.send("pipeline_output", output)


# =============================================================================
# Error Handling
# =============================================================================

@app.agent("errors", group="error_handlers")
async def error_handler(record: Message):
    """Handle errors from any pipeline stage."""
    msg = record.message
    print(f"[ErrorHandler] Error: {msg}")


# =============================================================================
# Monitoring
# =============================================================================

@app.timer(5)
async def pipeline_monitor():
    """Monitor pipeline health."""
    # In production, this would check queue depths, error rates, etc.
    health = await app.health_check()
    print(f"\n[Monitor] Pipeline status: {health['status']}")
    print(f"[Monitor] Active tasks: {health['active_tasks']}\n")


# =============================================================================
# Output Consumer (for demonstration)
# =============================================================================

@app.agent("pipeline_output", group="output")
async def output_consumer(record: Message):
    """Consume final pipeline output."""
    print(f"[FinalConsumer] Received: {json.dumps(record.message, indent=2)}\n")


if __name__ == "__main__":
    print("Starting pipeline pattern example...")
    print("This example demonstrates:")
    print("  - Producer → Raw → Validated → Transformed → Enriched → Output")
    print("  - Each stage processes and forwards to next stream")
    print("  - Error handling at each stage")
    print("\nPipeline stages:")
    print("  1. Producer: Raw data generation")
    print("  2. RawProcessor: Validation")
    print("  3. Transformer: Data transformation")
    print("  4. Enricher: Data enrichment")
    print("  5. Output: Final processing")
    print("\nPress Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")