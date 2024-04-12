import os
import time
from pathlib import Path
from datetime import datetime
from database.BaseDB import BaseDB
from concurrent.futures import ThreadPoolExecutor, as_completed

class LogDB(BaseDB):

    def __init__(self, source_path: Path, cores: int) -> None:
        super().__init__(source_path)
        self.__source_path = source_path
        self.__max_workers = cores

    def get_existing_logs(self) -> set:
        self.connect()
        try:
            if self.connection is not None and self.connection.is_connected():
                cursor = self.connection.cursor()
                cursor.execute("SELECT IDCompleto FROM logQuebraQlik")
                existing_zips = {row[0] for row in cursor.fetchall()}
                return existing_zips
        except Exception as e:
            print(f'Error on fetching existing log IDs: {e}')
            return set()
        finally:
            self.close()

    def update_table(self, files: list[Path]) -> None:
        start_time = time.time()
        self.create_table()
        print('Starting table insertion...')
      
        if files:
            pack_size = 50
            for i in range(0, len(files), pack_size):
                pack = files[i:i + pack_size]
                try:
                    with ThreadPoolExecutor(max_workers=self.__max_workers) as executor:
                        future_to_file = {executor.submit(self.__get_infos, file): file for file in pack}
                        info_files = []
                        for future in as_completed(future_to_file):
                            info_files.extend(future.result())

                    if info_files:
                        insert_query_path = os.path.join(self.__source_path, 'update_log.sql')
                        with open(insert_query_path, 'r') as file:
                            insert_query = file.read()
                        self.execute_query(insert_query, info_files)
                        print("Successful insertion.")
                    else:
                        print('No information to insert.')

                except Exception as e:
                    print(f'Error while updating table: {e}')

                finally:
                    self.close()

        end_time = time.time()
        print(f"Total execution time for updating table: {end_time - start_time:.2f} seconds")

    def __get_infos(self, file: Path) -> list[tuple]:

        parts = file.parts
        app = parts[-1].split('.')
        if len(parts) < 5:
            raise ValueError(f"Invalid file path structure: {file}")
        event = app[0]
        status = self.__get_status(file)
        status_code = 'OK' if 'Error:' not in status else 'Error'
        date = datetime.strptime(app[1][:8], '%Y%m%d').strftime('%Y-%m-%d')
        appID = file.stem
        path = str(Path(*parts[4:])).replace('\\', '/')
        return [(event, status, status_code, date, appID, path)]
    
    def __get_status(self, file: Path) -> str:
        content: list[str] = self.__open_file(file)
        content.reverse()

        for line in content:
            if "Error:" in line:
                error_index = line.find('Error:')
                return line[error_index:].strip()
            elif "Search index creation completed successfully" in line:
                status_message = "Search index creation completed successfully"
        
        return status_message if 'status_message' in locals() else "No message found."


    def __open_file(file: Path) -> list[str]:
        with open(file, 'r', encoding='utf-8') as f:
            return f.readlines()
