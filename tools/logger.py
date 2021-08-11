import logging
import os
from logging.handlers import RotatingFileHandler

# set up logging to file - see previous section for more details
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    #filename=os.path.join("..", "logs", "log.log"),
                    #filemode='w'
                    )
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
ppthf = os.path.join(".", "logs", "ic_log.log")
if not os.path.exists(os.path.join(".", "logs")):
    os.makedirs(os.path.join(".", "logs"))
filehandler = RotatingFileHandler(filename=os.path.join(".", "logs", "ic_log.log"), maxBytes=2000000, backupCount=20)
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
console.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
# add the handler to the root logger
logging.getLogger().addHandler(console)
logging.getLogger().addHandler(filehandler)

# Now, define a couple of other loggers which might represent areas in your
# application:
logger_rtapic = logging.getLogger('Investingclub.real_time_api_client')
logger_rtapi = logging.getLogger('Investingclub.real_time_api')
logger_get_price = logging.getLogger('Investingclub.get_price')
logger_get_ref = logging.getLogger('Investingclub.get_ref')
logger_get_analytics = logging.getLogger('Investingclub.get_analytics')
logger_get_opto = logging.getLogger('Investingclub.get_opto')