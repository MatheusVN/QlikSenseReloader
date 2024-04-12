from websocket import create_connection, WebSocket
from pathlib import Path
import json
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class QlikConnector:
    def __init__(self, host: str, apps_path: Path, cores: int) -> None:
        self.__host = host
        self.__apps_path = apps_path
        self.__apps = self.__find_apps()
        self.__max_workers = cores

    def execute_qlik(self):
        with ThreadPoolExecutor(max_workers=self.__max_workers) as executor:
            executor.map(self.__process_app, self.__apps)

    def __process_app(self, app: Path):
        ws = self.__conect_engine()
        if ws:
            self.__open_app(ws, app)
            self.__check_app_status(ws)
            self.__reload(ws)
            self.__save_app(ws)
            ws.close()

    def __conect_engine(self) -> WebSocket:
        logging.info("Connecting to Qlik Sense engine...")
        try:
            ws = create_connection(self.__host)
            logging.info("Connection established successfully!")
            return ws
        except Exception as e:
            logging.error("Error while connecting to the Qlik Sense engine. Make sure the Qlik Sense Desktop is open and logged in.")
            logging.error('Error details: %s', e)
            input('Press any key to close the window...')
            exit(1)
    
    def __open_app(self, ws: WebSocket, app: Path):
        logging.info(f'Openning app: {app.name}')
        try:
            request = {
                "jsonrpc": "2.0",
                "id": 0,
                "method": "OpenDoc",
                "handle": -1,
                "params": [
                    str(app),
                    ""
                ]
            }

            ws.send(json.dumps(request))
            response =  ws.recv()
            response =  json.loads(ws.recv())
            try:
                errorMessage = response["error"]["message"]
                logging.error(f"Error: {errorMessage}")
                ws.close()
                raise Exception(errorMessage)
            except:
                logging.info(f'App {app.name} opened successfully!')
        except Exception as e:
            logging.error(f'Error while opening the app: {e}')
            ws.close()
            raise
        
    def __check_app_status(self, ws: WebSocket):
        logging.info("Checking app status...")
        try:
            request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "GetActiveDoc",
                "handle": -1,
                "params": []
            }

            ws.send(json.dumps(request))
            response =  json.loads(ws.recv())
            try:
                errorMessage = response["error"]["message"]
                logging.error(f"Error: {errorMessage}")
                ws.close()
                raise Exception(errorMessage)
            except:
                logging.info('Checked app status!')
        except Exception as e:
            logging.error(f"Error while checking app status: {e}.")
            ws.close()
            raise

    def __reload(self, ws: WebSocket):
        logging.info("Starting reload...")
        try:
            request = {
                "handle": 1,
                "method": "DoReloadEx",
                "params": {},
                "id": 2,
                "jsonrpc": "2.0"
            }

            ws.send(json.dumps(request))
            response =  json.loads(ws.recv())
            try:
                errorMessage = response["error"]["message"]
                logging.error(f"Error: {errorMessage}")
                ws.close()
                raise Exception(errorMessage)
            except:
                pass
        except Exception as e:
            logging.error(f"Error while starting reload: {e}.")
            ws.close()
            raise

        try:
            status = response["result"]["qResult"]["qSuccess"]
            logging.info("Reload completed!")
        except:
            logging.error("Reload failed.")
            try:
                logFile = response["result"]["qResult"]["qScriptLogFile"]
                logging.info(f"See log at: {logFile}")
            except:
                logging.info("No log was generated.")
            ws.close()
            raise

    def __save_app(self, ws: WebSocket):
        logging.info("Saving app...")
        try:
            request = {
                "jsonrpc": "2.0",
                "id": 6,
                "method": "DoSave",
                "handle": 1,
                "params": []
            }

            ws.send(json.dumps(request))
            response =  json.loads(ws.recv())
            try:
                errorMessage = response["error"]["message"]
                logging.error(f"Error: {errorMessage}")
                ws.close()
                raise Exception(errorMessage)
            except:
                logging.info('App saved!')
        except Exception as e:
            logging.error(f"Error while saving the app: {e}.")
            ws.close()
            raise

    def __find_apps(self) -> list[Path]:
        return list(self.__apps_path.glob('*.qvf'))
