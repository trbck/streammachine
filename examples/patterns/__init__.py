"""
StreamMachine Pattern Examples

This package contains example patterns for common use cases:

- error_handling.py: Try/except, retry logic, dead letter queues
- backpressure.py: Queue monitoring, throttling, circuit breakers
- graceful_shutdown.py: Signal handling, cleanup, in-flight message handling
- state_management.py: Cross-agent state sharing, aggregation
- fanout_pattern.py: One producer with multiple consumer groups
- pipeline_pattern.py: Chain of agents processing messages

Each example can be run directly:
    python -m streammachine.examples.patterns.error_handling
"""