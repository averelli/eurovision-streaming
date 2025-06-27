import logging
import logging.handlers
import os

def setup_logging():
    logger = logging.getLogger("eurovision-pipeline")
    logger.setLevel(logging.INFO)  

    # Create a formatter
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s - module: %(module)s - function: %(funcName)s")

    # Log to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Ensure 'logs/' directory exists
    os.makedirs("logs", exist_ok=True)

    # Log to file with rotation
    file_handler = logging.handlers.TimedRotatingFileHandler(
        "logs/eurovision-pipeline.log",
        when="midnight",
        backupCount=7   
    )
    file_handler.setLevel(logging.INFO)  
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger