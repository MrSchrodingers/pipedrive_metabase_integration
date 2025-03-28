import logging
import sys
from pythonjsonlogger import jsonlogger

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    logHandler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    logHandler.setFormatter(formatter)
    
    # Limpa handlers antigos, se houver, e adiciona o novo
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(logHandler)

setup_logging()
