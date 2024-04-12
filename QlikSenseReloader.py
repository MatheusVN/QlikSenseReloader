from config.config import HOST, APPS_PATH, CPU_CORES, DB_PATH, LOGS_PATH
from utils.QlikConnector import QlikConnector
from database.LogDB import LogDB
import logging
import time

if __name__ == "__main__":
	logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
	try:
		start_time = time.time()
		qliker = QlikConnector(HOST, APPS_PATH)
		db = LogDB(DB_PATH, CPU_CORES)
		qliker.execute_qlik()

		logs = list(LOGS_PATH.rglob('*.log'))
		if logs:
			existing_logs = db.get_existing_logs()
			new_logs = [log for log in logs if log.stem not in existing_logs]

			if new_logs:
				db.update_table(new_logs)

		end_time = time.time()
		total_time = (end_time - start_time)/60.0
		print(f"Processing apps and logs was sucessfull. Total execution time: {total_time:.2f} minutes")

	except Exception as e:
		logging.info(f'Error on processing: {e}')