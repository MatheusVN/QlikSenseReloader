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

    def open_apps(self):
        for app in self.__apps:
            self.__conect_engine()
            self.__open_app(app)
            self.__check_app_status()
            self.__reload()
            self.__save_app()

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
        logging.info(f"Opening app {app.name}...")
        try:
            request = {
                "jsonrpc": "2.0",
                "id": 0,
                "method": "OpenDoc",
                "handle": -1,
                "params": [str(app), ""]
            }
            self.ws.send(json.dumps(request))
            response =  json.loads(self.ws.recv())

            if 'error' in response:
                errorMessage = response["error"]["message"]
                logging.error(f"Error: {errorMessage}")
                self.ws.close()
                raise Exception(errorMessage)
        
        except Exception as e:
            logging.error(f'Error while opening the app: {e}')
            self.ws.close()
            raise

        else:
            logging.info(f'App {app.name} opened successfully!')
        
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
            response = json.loads(self.ws.recv())
            if "error" in response:
                errorMessage = response["error"]["message"]
                logging.error(f"Error: {errorMessage}")
                self.ws.close()
                raise Exception(errorMessage)

        except Exception as e:
            logging.error("Error while checking app status.")
            logging.error(e)
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
            response = json.loads(self.ws.recv())
            if "error" in response:
                errorMessage = response["error"]["message"]
                logging.error(f"Error: {errorMessage}")
                self.ws.close()
                raise Exception(errorMessage)

        except Exception as e:
            logging.error("Error while starting reload.")
            logging.error(e)
            self.ws.close()
            raise

        else:
            self.__get_reload_status(response)
    
    def __get_reload_status(self, response):
            try:
                status = response["result"]["qResult"]["qSuccess"]
                logging.info("Reload completed")
            except:
                logging.error("Error. Reload failed.")
                try:
                    logFile = response["result"]["qResult"]["qScriptLogFile"]
                    logging.error(f"See log at {logFile}")
                except KeyError:
                    logging.error("No log was generated")
                    self.ws.close()
                raise Exception("Reload failed")

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
            response = json.loads(self.ws.recv())
            if "error" in response:
                errorMessage = response["error"]["message"]
                logging.error(f"Error: {errorMessage}")
                self.ws.close()
                raise Exception(errorMessage)

        except Exception as e:
            logging.error("Error while saving the app.")
            logging.error(e)
            self.ws.close()
            raise

    def __find_apps(self) -> list[Path]:
        return list(self.__apps_path.glob('*.qvf'))
