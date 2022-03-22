import json
import os
import shutil
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

    def __init__(self, in_coordination_file_path: str,
                 in_result_file_path: str,
                 in_archive_file_path: str,
                 in_osrm_server_base_url: str):
        self.coordination_file_path = in_coordination_file_path
        self.result_file_path = in_result_file_path
        self.archive_file_path = in_archive_file_path
        self.osrm_server_base_url = in_osrm_server_base_url

    def start(self):
        logger.info(f"Calculating Distance and Duration for coordination file '{self.coordination_file_path}'")

        if not os.path.exists(self.coordination_file_path):
            raise Exception(f"The coordination file '{self.coordination_file_path}' does not exists!")

        coordination_list = self._get_coordination_list()

        self._calculate_distance_duration(coordination_list)

    def _get_coordination_list(self) -> [List[Dict]]:

        all_df = pd.read_csv(self.coordination_file_path, dtype=CoordinationFileDbTypes, sep=";")
        if os.path.exists(self.result_file_path):
            proceed_df = pd.read_csv(self.result_file_path, dtype=DistanceDurationDbTypes, sep=";")
            dfe = pd.merge(all_df, proceed_df, how='left', on=['key1', 'key2'], indicator=True)
            all_df = dfe[dfe["_merge"] == "left_only"]

            logger.debug(f"In '{self.coordination_file_path}' {len(proceed_df.index)} are proceed. It remains {len(all_df.index)} to calculate.")

        pd_data_list = all_df.to_dict(orient='records')

        return pd_data_list

    def _calculate_distance_duration(self, coordination_list: List[Dict]):

        total_items = len(coordination_list)
        logger.debug(f"Retrieve {total_items} distances from the coordination list ...")
        start_time = time.time()
        last_proceed_count = 0

        pd_data = {
            CoordinationRequestSchema.key1: [],
            CoordinationRequestSchema.key2: [],
            DistanceDurationSchema.distance: [],
            DistanceDurationSchema.duration: []
        }

        for coord_item in coordination_list:
            # logger.debug(f"Processing the kh_key '{kh_key_item['kh_key']}'")
            key1 = coord_item[CoordinationRequestSchema.key1]
            key2 = coord_item[CoordinationRequestSchema.key2]

            distance, duration = self._get_driving_distance(self.osrm_server_base_url,
                                                            coord_item[CoordinationRequestSchema.longitude1],
                                                            coord_item[CoordinationRequestSchema.latitude1],
                                                            coord_item[CoordinationRequestSchema.longitude2],
                                                            coord_item[CoordinationRequestSchema.latitude2])
            pd_data[CoordinationRequestSchema.key1].append(key1)
            pd_data[CoordinationRequestSchema.key2].append(key2)
            pd_data[DistanceDurationSchema.distance].append(distance)
            pd_data[DistanceDurationSchema.duration].append(duration)

            last_proceed_count += 1

            if last_proceed_count >= self.writing_chunk:
                self._write_results_in_csv(pd_data, total_items)

                last_proceed_count = 0

        if last_proceed_count > 0:
            self._write_results_in_csv(pd_data, total_items)

        logger.debug(f"End of Retrieve {total_items} distances from the coordination list.")
        shutil.move(self.coordination_file_path, self.archive_file_path)
        logger.debug(f"File '{self.coordination_file_path}' moved to archive: '{self.archive_file_path}'")

        logger.debug(f"Retrieve {total_items} distances from the coordination list is done. {(time.time() - start_time)}")

    def _write_results_in_csv(self, pd_data, total_items):
        logger.debug(
            f"Write {len(pd_data[CoordinationRequestSchema.key1])} from {total_items} in '{self.result_file_path}' ...")
        if os.path.exists(self.result_file_path):
            os.remove(self.result_file_path)
        result_file_parent_folder = os.path.dirname(self.result_file_path)
        if not os.path.exists(result_file_parent_folder):
            os.mkdir(result_file_parent_folder)
        pd_df = pd.DataFrame.from_dict(pd_data)
        pd_df.to_csv(self.result_file_path, index=False, sep=";")

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
    coordination_file_path = "/mnt/daten/distance_place_temp/coordination_items_0000000028.csv"
    archive_file_path = "/mnt/daten/distance_place_temp/archive/coordination_items_0000000028.csv"
    result_file_path = "/mnt/daten/distance_place_temp/result/results_items_0000000028.csv"
    osrm_server_base_url = "http://localhost:5000"
    for arg in sys.argv:
        if arg.lower().startswith("coord_file="):
            coordination_file_path = arg.lower().replace("coord_file=", "").strip()
        if arg.lower().startswith("archive_file="):
            archive_file_path = arg.lower().replace("archive_file=", "").strip()
        if arg.lower().startswith("result_file="):
            result_file_path = arg.lower().replace("result_file=", "").strip()
        if arg.lower().startswith("osrm_url="):
            osrm_server_base_url = arg.lower().replace("osrm_url=", "").strip()

    if coordination_file_path is None:
        raise Exception(f"coord_file argument is not set!")
    if result_file_path is None:
        raise Exception(f"result_file argument is not set!")
    if archive_file_path is None:
        raise Exception(f"result_file argument is not set!")
    if osrm_server_base_url is None:
        raise Exception(f"Invalid osrm_url!")

    DistanceDurationCalculationFileApp(in_coordination_file_path=coordination_file_path,
                                       in_result_file_path=result_file_path,
                                       in_archive_file_path=archive_file_path,
                                       in_osrm_server_base_url=osrm_server_base_url).start()
