from loguru import logger
from utils import get_data, save_statistics

logger.add(".log", rotation="1 MB")

data = get_data()
save_statistics(data)