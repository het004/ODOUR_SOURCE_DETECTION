import logging
import os

# Create 'logs' directory if not exists
if not os.path.exists('logs'):
    os.makedirs('logs')

# Logging configuration
logging.basicConfig(
    filename='logs/app.log',  # Log file
    level=logging.INFO,       # Log level: INFO and above
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_logger(name):
    return logging.getLogger(name)
