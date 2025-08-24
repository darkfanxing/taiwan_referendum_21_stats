from loguru import logger
from utils import get_referendum_data, save_referendum_statistics, get_legislators_data, save_legislators_statistics

logger.add(".log", rotation="1 MB")

data = get_referendum_data()
save_referendum_statistics(data)

legislators_data = get_legislators_data()
save_legislators_statistics(legislators_data)