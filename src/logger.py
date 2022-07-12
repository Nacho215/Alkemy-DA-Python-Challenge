"""
    This module set up a basic logger system.
    It has a simple method that save logs into a default log path
    specified in the settings .env file.
"""

#Logging
import logging
#Settings module
import settings

#Set up logging to save logs into default_log_path
logging.basicConfig(
    filename=settings.default_log_path, 
    level=logging.INFO, encoding='utf_8',
    format='%(asctime)s (%(levelname)s): %(message)s',
    datefmt='%m/%d/%Y %H:%M:%S'
)
#Save the logger
logger = logging.getLogger(__name__)

#Methods
def log(level_name, msg, exc_info=""):
    """Log a message with an given level.

    Args:
        level_name (str): A string representing a log level: 'INFO', 'ERROR' or 'DEBUG'
        msg (str): Message to log
        exc_info (str, optional): Exception message, only if level_name = 'ERROR'. Defaults to "".
    """
    #Get the level
    match level_name:
        case 'INFO' : level = logging.INFO
        case 'ERROR' : level = logging.ERROR
        case 'DEBUG' : level = logging.DEBUG
        case _: level = logging.INFO
    #Log the msg
    logger.log(level=level, msg=msg, exc_info=exc_info)