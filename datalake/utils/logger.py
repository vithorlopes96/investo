import structlog


# Logger setup using structlog (SRP)
def setup_logger():
    """Sets up the structured logger using structlog."""
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()  # Logs in JSON format
        ]
    )
    logger = structlog.get_logger()
    return logger
