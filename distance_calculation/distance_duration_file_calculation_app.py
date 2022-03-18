import json
import os
import sys
from typing import Dict, List

import pandas as pd
from loguru import logger

import requests
import time

from coordination_schmea import CoordinationRequestSchema, DistanceDurationSchema, \
    CoordinationFileDbTypes, DistanceDurationDbTypes


class DistanceDurationCalculationFileApp:
    writing_chunk = 2000

    def __init__(self, coordination_file_path: str, result_file_path: str, osrm_server_base_url: str):
        self.coordination_file_path = coordination_file_path
        self.result_file_path = result_file_path
        self.osrm_server_base_url = osrm_server_base_url

    def start(self):
        logger.info(f"Calculating Distance and Duration for coordination file '{self.coordination_file_path}'")

        if not os.path.exists(self.coordination_file_path):
            raise Exception(f"The coordination file '{self.coordination_file_path}' does not exists!")

        proceed_data_list = self._extract_proceed_data_list()

        coordination_list = self._get_coordination_list(self.coordination_file_path)

        self._calculate_distance_duration(coordination_list, proceed_data_list)

    def _extract_proceed_data_list(self):
        proceed_data_list = []
        logger.debug(f"Extract proceed data list from '{self.result_file_path}' ...")
        if os.path.exists(self.result_file_path):
            pd_df = pd.read_csv(self.result_file_path, dtype=DistanceDurationDbTypes, sep=";")
            proceed_data_list = pd_df.to_dict(orient='records')
        return proceed_data_list

    @staticmethod
    def _get_coordination_list(coordination_file_path: str) -> [List[Dict]]:

        pd_df = pd.read_csv(coordination_file_path, dtype=CoordinationFileDbTypes, sep=";")
        pd_data_list = pd_df.to_dict(orient='records')

        return pd_data_list

    def _calculate_distance_duration(self, coordination_list: List[Dict], proceed_data_list):

        total_items = len(coordination_list)
        logger.debug(f"Retrieve {total_items} distances from the coordination list ...")
        start_time = time.time()
        last_proceed_count = 0

        pd_data = {
            CoordinationRequestSchema.key1.NAME: [],
            CoordinationRequestSchema.key2.NAME: [],
            DistanceDurationSchema.distance.NAME: [],
            DistanceDurationSchema.duration.NAME: []
        }

        for coord_item in coordination_list:
            # logger.debug(f"Processing the kh_key '{kh_key_item['kh_key']}'")
            key1 = coord_item[CoordinationRequestSchema.key1.NAME]
            key2 = coord_item[CoordinationRequestSchema.key2.NAME]

            proceed_res = [p for p in proceed_data_list if
                           (p[CoordinationRequestSchema.key1.NAME] == key1)
                           and (p[CoordinationRequestSchema.key2.NAME] == key2)]
            if len(proceed_res) > 0:
                pd_data[CoordinationRequestSchema.key1.NAME].append(key1)
                pd_data[CoordinationRequestSchema.key2.NAME].append(key2)
                pd_data[DistanceDurationSchema.distance.NAME].append(proceed_res[0][DistanceDurationSchema.distance.NAME])
                pd_data[DistanceDurationSchema.duration.NAME].append(proceed_res[0][DistanceDurationSchema.duration.NAME])

                continue

            distance, duration = self._get_driving_distance(self.osrm_server_base_url,
                                                            coord_item[CoordinationRequestSchema.longitude1.NAME],
                                                            coord_item[CoordinationRequestSchema.latitude1.NAME],
                                                            coord_item[CoordinationRequestSchema.longitude2.NAME],
                                                            coord_item[CoordinationRequestSchema.latitude2.NAME])
            pd_data[CoordinationRequestSchema.key1.NAME].append(key1)
            pd_data[CoordinationRequestSchema.key2.NAME].append(key2)
            pd_data[DistanceDurationSchema.distance.NAME].append(distance)
            pd_data[DistanceDurationSchema.duration.NAME].append(duration)

            last_proceed_count += 1

            if last_proceed_count >= self.writing_chunk:
                logger.debug(f"Write {len(pd_data[CoordinationRequestSchema.key1.NAME])} from {total_items} in '{self.result_file_path}' ...")

                if os.path.exists(self.result_file_path):
                    os.remove(self.result_file_path)

                result_file_parent_folder = os.path.dirname(self.result_file_path)
                if not os.path.exists(result_file_parent_folder):
                    os.mkdir(result_file_parent_folder)

                pd_df = pd.DataFrame.from_dict(pd_data)
                pd_df.to_csv(self.result_file_path, index=False, sep=";")

                last_proceed_count = 0

        logger.debug(f"End of loop!")

        if last_proceed_count > 0:
            if os.path.exists(self.result_file_path):
                os.remove(self.result_file_path)

            pd_df = pd.DataFrame.from_dict(pd_data)
            pd_df.to_csv(self.result_file_path, index=False)

        logger.debug(f"Retrieve {total_items} distances from the coordination list is done. {(time.time() - start_time)}")

    @staticmethod
    def _get_driving_distance(server_base_url: str, long_src: str, lat_src: str, long_tgt: str, lat_tgt: str):
        loc = f'{long_src},{lat_src};{long_tgt},{lat_tgt}'
        url = f"{server_base_url}/route/v1/driving/{loc}?skip_waypoints=true&overview=false"

        try_count = 1
        content = None
        while try_count < 20:
            r = requests.get(url)
            if r.status_code == 200:
                content = r.content
                break
            logger.error(f"Received API error (Status: {r.status_code}). try({try_count})")

            if try_count >= 10:
                raise Exception(f"Receive router API send {try_count} times error! The process will be stopped.")
            try_count += 1

        routes = json.loads(content).get("routes")[0]
        distance = float(routes["distance"]) / 1000
        duration = float(routes["duration"]) / 60

        distance = DistanceDurationCalculationFileApp._round_float(distance)
        duration = DistanceDurationCalculationFileApp._round_float(duration)

        # time.sleep(1)
        return distance, duration

    @staticmethod
    def _round_float(f_value: float):
        f_value_str = "%.2f" % f_value
        f_value = float(f_value_str)
        return f_value


if __name__ == '__main__':
    coordination_file_path = None #"/mnt/daten/distance_place_temp/coordination_items_0000000000.csv"
    result_file_path = None #"/mnt/daten/distance_place_temp/result/results_items_0000000000.csv"
    osrm_server_base_url = "http://localhost:5000"
    for arg in sys.argv:
        if arg.lower().startswith("coord_file="):
            coordination_file_path = arg.lower().replace("coord_file=", "").strip()
        if arg.lower().startswith("result_file="):
            result_file_path = arg.lower().replace("result_file=", "").strip()
        if arg.lower().startswith("osrm_url="):
            osrm_server_base_url = arg.lower().replace("osrm_url=", "").strip()

    if coordination_file_path is None:
        raise Exception(f"coord_file argument is not set!")
    if result_file_path is None:
        raise Exception(f"result_file argument is not set!")
    if osrm_server_base_url is None:
        raise Exception(f"Invalid osrm_url!")

    DistanceDurationCalculationFileApp(coordination_file_path=coordination_file_path,
                                       result_file_path=result_file_path,
                                       osrm_server_base_url=osrm_server_base_url).start()
