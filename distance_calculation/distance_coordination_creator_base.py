import os
from typing import Dict, List

import pandas as pd
from loguru import logger

from de.mediqon.apps.geografie.distance_calculation.coordination_schmea import CoordinationItemSchema, CoordinationRequestSchema, DistanceCoordinationFileHelper
import time


class DistanceCoordinationCreatorBase:

    def __init__(self, read_data_chunk_size: int, proceed_saving_folder: str, if_exists: str):
        self.read_data_chunk_size = read_data_chunk_size
        self.proceed_saving_folder = proceed_saving_folder
        self.if_exists = if_exists

    @staticmethod
    def create_coordination_item(key: str,
                                 longitude: float,
                                 latitude: float) -> Dict:
        item = {
            CoordinationItemSchema.key: key,
            CoordinationItemSchema.longitude: longitude,
            CoordinationItemSchema.latitude: latitude
        }

        return item

    def process_distance_standort_data(self,
                                       point_list_1: List[Dict],
                                       point_list_2: List[Dict],
                                       delete_old_files: bool = False):

        total_items = len(point_list_1) * len(point_list_2)
        logger.debug(f"Create coordination's list ...")
        start_time = time.time()

        if delete_old_files:
            old_files = [os.path.join(self.proceed_saving_folder, f)
                         for f in os.listdir(self.proceed_saving_folder) if f.lower().endswith(".csv")]
            for old_file in old_files:
                if os.path.isfile(old_file) or os.path.islink(old_file):
                    os.unlink(old_file)

        proceed_items = {
            CoordinationRequestSchema.key1: [],
            CoordinationRequestSchema.longitude1: [],
            CoordinationRequestSchema.latitude1: [],
            CoordinationRequestSchema.key2: [],
            CoordinationRequestSchema.longitude2: [],
            CoordinationRequestSchema.latitude2: []
        }

        saving_index = 0

        for point_item_1 in point_list_1:
            # logger.debug(f"Processing the kh_key '{kh_key_item['kh_key']}'")
            key_item_1 = point_item_1[CoordinationItemSchema.key]
            longitude_item_1 = point_item_1[CoordinationItemSchema.longitude]
            latitude_1 = point_item_1[CoordinationItemSchema.latitude]

            t = 0
            # logger.debug(f"Processing kh_key: '{kh_key_item['kh_key']}' ...")
            for point_item_2 in point_list_2:
                # logger.debug(f"Processing the plz '{plz_item['plz']}'")
                key_item_2 = point_item_2[CoordinationItemSchema.key]
                longitude_item_2 = point_item_2[CoordinationItemSchema.longitude]
                latitude_2 = point_item_2[CoordinationItemSchema.latitude]

                proceed_items[CoordinationRequestSchema.key1].append(key_item_1)
                proceed_items[CoordinationRequestSchema.longitude1].append(longitude_item_1)
                proceed_items[CoordinationRequestSchema.latitude1].append(latitude_1)
                proceed_items[CoordinationRequestSchema.key2].append(key_item_2)
                proceed_items[CoordinationRequestSchema.longitude2].append(longitude_item_2)
                proceed_items[CoordinationRequestSchema.latitude2].append(latitude_2)

                if len(proceed_items[CoordinationRequestSchema.key1]) >= self.read_data_chunk_size:
                    self._save_proceed_items(saving_items=proceed_items,
                                             index=saving_index,
                                             total_items=total_items,
                                             if_exists=self.if_exists)
                    saving_index += 1

                    proceed_items = {
                        CoordinationRequestSchema.key1: [],
                        CoordinationRequestSchema.longitude1: [],
                        CoordinationRequestSchema.latitude1: [],
                        CoordinationRequestSchema.key2: [],
                        CoordinationRequestSchema.longitude2: [],
                        CoordinationRequestSchema.latitude2: []
                    }

                    # t += 1
                    # if t > 3:
                    #    break

            # time.sleep(5)
        if len(proceed_items[CoordinationRequestSchema.key1]) > 0:
            self._save_proceed_items(saving_items=proceed_items,
                                     index=saving_index,
                                     total_items=total_items,
                                     if_exists="delete")

        logger.debug(f"Retrieve distance of standorte from the kh_key and plz list done. {(time.time() - start_time)}")

    def _save_proceed_items(self, saving_items: Dict, index: int, total_items: int, if_exists: str = ""):
        file_path = self._get_new_temp_file_path(index)

        wrote_size = self.read_data_chunk_size * index
        logger.debug(f"Writing {len(saving_items[CoordinationRequestSchema.key1]) + wrote_size} from {total_items} items in: '{file_path}'")

        if os.path.exists(file_path):
            if (if_exists.lower() == "delete") or (if_exists.lower() == "overwrite"):
                os.remove(file_path)
            if if_exists.lower() == "error":
                raise Exception(f"The coordination file '{file_path}' exists!")

        saving_pd_df = pd.DataFrame.from_dict(saving_items)
        saving_pd_df.to_csv(file_path, index=False, sep=";")

    def _get_new_temp_file_path(self, index: int):
        index_str = "%010d" % (index,)
        filepath = os.path.join(self.proceed_saving_folder, f"{DistanceCoordinationFileHelper.coordination_file_prefix}{index_str}.csv")
        return filepath
