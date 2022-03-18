import json
import os
import subprocess
import sys
from typing import Dict, List

import pandas as pd
from loguru import logger


from distance_coordination_creator_base import DistanceCoordinationCreatorBase


class DistanceDurationCalculationAllApp:
    max_running_items = 10
    running_process = []
    not_proceed_coordination_files_list = []
    proceed_coordination_files_list = []

    process_coord_file_script_path = ""

    def __init__(self, coordination_root_path: str, result_root_path: str, osrm_server_base_url: str):
        self.coordination_root_path = coordination_root_path
        self.result_root_path = result_root_path
        self.osrm_server_base_url = osrm_server_base_url

        self.process_coord_file_script_path = os.path.join(os.path.dirname(__file__), "distance_duration_file_calculation_app.py")

    def start(self):
        logger.info(f"Calculating Distance and Duration for coordination files in '{self.coordination_root_path}'")

        if not os.path.exists(self.coordination_root_path):
            raise Exception(f"The coordination files folder '{self.coordination_root_path}' does not exists!")

        self.not_proceed_coordination_files_list = self._extract_coordination_files_list()

        process_still_running = True
        while process_still_running:

            if (len(self.not_proceed_coordination_files_list) == 0) and (len(self.running_process) == 0):
                logger.info("All coordination files process are finished.")
                break

            if len(self.not_proceed_coordination_files_list) > 0:
                while len(self.running_process) < self.max_running_items:
                    coord_item = self.not_proceed_coordination_files_list[0]
                    self.not_proceed_coordination_files_list.remove(coord_item)

                    running_arguments = ["python",
                                         self.process_coord_file_script_path,
                                         f"coord_file={coord_item['source']}",
                                         f"result_file={coord_item['result']}",
                                         f"osrm_url={self.osrm_server_base_url}"]

                    coord_item["status"] = "running"
                    coord_item["proc"] = subprocess.Popen(running_arguments)

                    self.running_process.append(coord_item)
                    logger.info(f"Start processing {coord_item['source']}")

            for running_item in self.running_process:
                if running_item["proc"].poll() is not None:
                    return_code = running_item["proc"].returncode
                    if return_code == 0:
                        running_item["status"] = "done"
                    else:
                        running_item["status"] = "error"
                    self.running_process.remove(running_item)
                    self.proceed_coordination_files_list.append(running_item)

                    logger.info(f"End of processing {running_item['source']} with {running_item['status']}")

        succeed_items = [i for i in self.proceed_coordination_files_list if i["status"] == "done"]
        failed_items = [i for i in self.proceed_coordination_files_list if i["status"] == "done"]

        logger.info(f"All process are finished")
        logger.info(f"{len(succeed_items)} process are succeeded")
        logger.info(f"{len(failed_items)} process are failed")
        logger.info(f"Failed items:")
        for i in failed_items:
            logger.info(f"   - {i['source']}  -->  {i['result']}")

    def _extract_coordination_files_list(self) -> List[Dict]:
        files_list = [f for f in os.listdir(self.coordination_root_path) if f.lower().endswith(".csv") and
                      f.lower().startswith(DistanceCoordinationCreatorBase.coordination_file_prefix)]

        files_list = [{"source": os.path.join(self.coordination_root_path, f),
                       "result": self._extract_result_file_name(f),
                       "status": False,
                       "proc": False} for f in files_list]

        return files_list

    def _extract_result_file_name(self, source_file_name) -> str:

        file_index = source_file_name.replace(DistanceCoordinationCreatorBase.coordination_file_prefix, "").replace(".csv",
                                                                                                                 "")
        return os.path.join(self.result_root_path, f"results_items_{file_index}.csv")


if __name__ == '__main__':
    coordination_root_path = "/mnt/daten/distance_place_temp"
    result_root_path = "/mnt/daten/distance_place_temp/result"
    osrm_server_base_url = "http://localhost:5000"
    for arg in sys.argv:
        if arg.lower().startswith("coord_folder="):
            coordination_root_path = arg.lower().replace("coord_folder=", "").strip()
        if arg.lower().startswith("result_folder="):
            result_root_path = arg.lower().replace("result_folder=", "").strip()
        if arg.lower().startswith("osrm_url="):
            osrm_server_base_url = arg.lower().replace("osrm_url=", "").strip()

    if coordination_root_path is None:
        raise Exception(f"coord_file argument is not set!")
    if result_root_path is None:
        raise Exception(f"result_file argument is not set!")
    if osrm_server_base_url is None:
        raise Exception(f"Invalid osrm_url!")

    DistanceDurationCalculationAllApp(coordination_root_path=coordination_root_path,
                                      result_root_path=result_root_path,
                                      osrm_server_base_url=osrm_server_base_url).start()
