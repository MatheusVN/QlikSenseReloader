from config.config import HOST, APPS_PATH
from utils.QlikConnector import QlikConnector
import logging

if __name__ == "__main__":
	logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
	try:
		qliker = QlikConnector(HOST, APPS_PATH)
		qliker.open_apps()

	except Exception as e:
		logging.info(f'Error on openning apps: {e}')