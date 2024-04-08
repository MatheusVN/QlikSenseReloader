from websocket import create_connection
from pathlib import Path
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class QlikConnector:
    def __init__(self, host: str, apps_path: Path) -> None:
        self.__host = host
        self.__apps_path = apps_path
        self.ws = None
        self.__apps = self.__find_apps()
        self.error = False

    def execute_qlik(self):
        for app in self.__apps:
            self.__conect_engine()
            self.__open_app(app)
            self.__check_app_status()
            self.__reload()
            self.__save_app()
            self.ws.close()

    def __conect_engine(self):
        logging.info("Connecting to Qlik Sense engine...")
        try:
            self.ws = create_connection(self.__host)
            logging.info("Connection established successfully!")
        except Exception as e:
            logging.error("Error while connecting to the Qlik Sense engine. Make sure the Qlik Sense Desktop is open and logged in.")
            logging.error('Error details: %s', e)
            input('Press any key to close the window...')
            exit(1)
    
    def __open_app(self, app: Path):
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

            self.ws.send(json.dumps(request))
            response =  self.ws.recv()
            response =  json.loads(self.ws.recv())
            try:
                errorMessage = response["error"]["message"]
                logging.error(f"Error: {errorMessage}")
                self.ws.close()
                raise Exception(errorMessage)
            except:
                logging.info(f'App {app.name} opened successfully!')
        except Exception as e:
            logging.error(f'Error while opening the app: {e}')
            self.ws.close()
            raise
        
    def __check_app_status(self):
        logging.info("Checking app status...")
        try:
            request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "GetActiveDoc",
                "handle": -1,
                "params": []
            }

            self.ws.send(json.dumps(request))
            response =  json.loads(self.ws.recv())
            try:
                errorMessage = response["error"]["message"]
                logging.error(f"Error: {errorMessage}")
                self.ws.close()
                raise Exception(errorMessage)
            except:
                logging.info('Checked app status!')
        except Exception as e:
            logging.error(f"Error while checking app status: {e}.")
            self.ws.close()
            raise

    def __reload(self):
        logging.info("Starting reload...")
        try:
            request = {
                "handle": 1,
                "method": "DoReloadEx",
                "params": {},
                "id": 2,
                "jsonrpc": "2.0"
            }

            self.ws.send(json.dumps(request))
            response =  json.loads(self.ws.recv())
            try:
                errorMessage = response["error"]["message"]
                logging.error(f"Error: {errorMessage}")
                self.ws.close()
                raise Exception(errorMessage)
            except:
                pass
        except Exception as e:
            logging.error(f"Error while starting reload: {e}.")
            self.ws.close()
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
            self.ws.close()
            raise

    def __save_app(self):
        logging.info("Saving app...")
        try:
            request = {
                "jsonrpc": "2.0",
                "id": 6,
                "method": "DoSave",
                "handle": 1,
                "params": []
            }

            self.ws.send(json.dumps(request))
            response =  json.loads(self.ws.recv())
            try:
                errorMessage = response["error"]["message"]
                logging.error(f"Error: {errorMessage}")
                self.ws.close()
                raise Exception(errorMessage)
            except:
                logging.info('App saved!')
        except Exception as e:
            logging.error(f"Error while saving the app: {e}.")
            self.ws.close()
            raise

    def __find_apps(self) -> list[Path]:
        return list(self.__apps_path.glob('*.qvf'))
