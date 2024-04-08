from config.config import HOST, APPS_PATH, CPU_CORES
from utils.QlikConnector import QlikConnector
import logging

if __name__ == "__main__":
	logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
	try:
		qliker = QlikConnector(HOST, APPS_PATH)
		qliker.execute_qlik()

	except Exception as e:
		logging.info(f'Error on openning apps: {e}')