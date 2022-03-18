import json
import os
import shutil
import uuid
from datetime import date
from typing import Dict, List

import pandas as pd
from loguru import logger

import geopandas
import requests
from geopandas import GeoDataFrame
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType

from de.mediqon.core.spark_app import SparkApp
from de.mediqon.etl.kh_key_list import KhKeys
from de.mediqon.etl.read.db_reader import DatabaseReader
from de.mediqon.etl.schemas.source.krankenhaus.qb_kh_stamm_source_schema import QB_KH_STAMM_SOURCE_TABLE, \
    QbKhStammSourceSchema
from de.mediqon.etl.schemas.source.geografie.distanzen_standort_plz_schema import DISTANZEN_STANDORT_PLZ_SOURCE_TABLE, \
    DistanzenStandortPlzSourceSchema
from de.mediqon.etl.schemas.tableau_geografie.distanzen_standort_plz import DISTANZEN_STANDORT_PLZ_TABLE, \
    DistanzenStandortPlzSchema
from de.mediqon.etl.schemas.tableau_geografie.geografie_basic import GEOGRAFIE_BASIC_TABLE, GeografieBasicSchema
import time

from de.mediqon.etl.write.db_writer import DatabaseWriter
from de.mediqon.etl.write.generic_versioning import GenericVersioning


class DistanceCalculationApp(SparkApp):

    def __init__(self):
        super(DistanceCalculationApp, self).__init__(app_name="Distance Calculation")
    source_table = DISTANZEN_STANDORT_PLZ_SOURCE_TABLE
    tableau_table = DISTANZEN_STANDORT_PLZ_TABLE

    proceed_temp_path = "/mnt/daten/distance_place_temp"
    read_data_chunk_size = 1000

    def run_app(self, *args, **kwargs):

        logger.info(f"Calculating Distance's")

        reset_data = kwargs.get("reset", None)
        if reset_data is not None:
            if (reset_data == True) or (str(reset_data).lower() == "true"):
                reset_data = True
            else:
                reset_data = False
        else:
            reset_data = False

        proceed_items_list, proceed_kh_key_list = self._get_proceed_list(reset_data)
        kh_key_list = self._get_kh_key_list()
        plz_list = self._get_plz_list()

        self._process_distance_standort_data(kh_key_list, plz_list, proceed_items_list, proceed_kh_key_list)

        #self._write_without_versioning(proceed_temp_df)

    def _get_proceed_list(self, reset_data: bool) -> [List[str], List[str]]:
        if not os.path.exists(self.proceed_temp_path):
            os.mkdir(self.proceed_temp_path)

        if reset_data:
            shutil.rmtree(self.proceed_temp_path)
            os.mkdir(self.proceed_temp_path)

        data_files = self._get_temp_file_list()

        proceed_items_list = []
        for f in data_files:
            pd_df = pd.read_csv(f, dtype=str)
            pd_dict = pd_df.to_dict(orient='records')
            lst = [r["kh_key"] + "_" + str(r["plz"]) for r in pd_dict]
            proceed_items_list += lst

        filepath = self._get_proceed_kh_key_file()
        proceed_kh_key_list = []
        if os.path.exists(filepath):
            pd_df = pd.read_csv(filepath, dtype=str)
            pd_dict = pd_df.to_dict(orient='records')
            proceed_kh_key_list = [r["item"] for r in pd_dict]

        logger.debug(f"There is {len(proceed_items_list)} items in temp proceed_items_list.")
        logger.debug(f"There is {len(proceed_kh_key_list)} items in proceed_kh_key_list.")
        return proceed_items_list, proceed_kh_key_list

    def _process_created_temp_files(self) -> DataFrame:

        data_files = self._get_temp_file_list()

        pd_df_list = []
        for f in data_files:
            pd_df = pd.read_csv(f, dtype=str)
            pd_df_list.append(pd_df)

        pd_total_df = pd.concat(pd_df_list)
        proceed_temp_df = self.spark.createDataFrame(pd_total_df)

        logger.debug(f"There is {proceed_temp_df.count()} items in temp proceed_list.")

        for f in data_files:
            os.remove(f)

        return proceed_temp_df

    def _get_temp_file_list(self):
        files = os.listdir(self.proceed_temp_path)
        data_files = [os.path.join(self.proceed_temp_path, f) for f in files if f.startswith("distance_standort_")
                      and f.lower().endswith(".csv")]
        return data_files

    def _process_distance_standort_data(self,
                                        kh_key_list: List,
                                        plz_list: List,
                                        proceed_items_list: List[str],
                                        proceed_kh_key_list: List[str]):

        total_items = len(kh_key_list) * len(plz_list)
        logger.debug(f"Retrieve {total_items} distances from the kh_key and plz list ...")
        start_time = time.time()
        log_index = 0

        pd_data = {
            "kh_key": [],
            "latitude_kh": [],
            "longitude_kh": [],
            "plz": [],
            "latitude_plz": [],
            "longitude_plz": [],
            "fahrzeit_min": [],
            "fahrstrecke_km": [],
        }

        for kh_key_item in kh_key_list:
            #logger.debug(f"Processing the kh_key '{kh_key_item['kh_key']}'")
            khkey_compare_key = self._get_khkey_compare_key(kh_key_item)
            if khkey_compare_key in proceed_kh_key_list:
                print(f"The kh_key: '{kh_key_item['kh_key']} is proceed before !!!")
                continue

            t = 0
            logger.debug(f"Processing kh_key: '{kh_key_item['kh_key']}' ...")
            for plz_item in plz_list:
                #logger.debug(f"Processing the plz '{plz_item['plz']}'")
                key = kh_key_item['kh_key'] + "_" + plz_item['plz']
                if key in proceed_items_list:
                    print(f"The kh_key: '{kh_key_item['kh_key']} and plz: '{plz_item['plz']}' is proceed before !!!")
                    continue

                distance, duration = self._get_driving_distance(kh_key_item['longitude_kh'],
                                                                kh_key_item['latitude_kh'],
                                                                plz_item["longitude_plz"],
                                                                plz_item["latitude_plz"])

                pd_data["kh_key"].append(kh_key_item['kh_key'])
                pd_data["latitude_kh"].append(kh_key_item['latitude_kh'])
                pd_data["longitude_kh"].append(kh_key_item['longitude_kh'])
                pd_data["plz"].append(plz_item['plz'])
                pd_data["latitude_plz"].append(plz_item['latitude_plz'])
                pd_data["longitude_plz"].append(plz_item['longitude_plz'])
                pd_data["fahrzeit_min"].append(distance)
                pd_data["fahrstrecke_km"].append(duration)

                log_index += 1

                if log_index == 10:
                    print(f"Data in list: {len(pd_data['kh_key'])} kh_key: '{kh_key_item['kh_key']}' ")
                    log_index = 0

                if len(pd_data["kh_key"]) >= self.read_data_chunk_size:

                    filepath = self._get_new_temp_file_path()

                    logger.debug(f"Writing {len(pd_data['kh_key'])} temp data in file: {filepath}")

                    pd_df = pd.DataFrame.from_dict(pd_data)
                    pd_df.to_csv(filepath, index=False)

                    pd_data = {
                        "kh_key": [],
                        "latitude_kh": [],
                        "longitude_kh": [],
                        "plz": [],
                        "latitude_plz": [],
                        "longitude_plz": [],
                        "fahrzeit_min": [],
                        "fahrstrecke_km": [],
                    }
                    #t += 1
                    #if t > 3:
                    #    break

            if len(pd_data["kh_key"]) >= 0:
                filepath = self._get_new_temp_file_path()

                logger.debug(f"Writing {len(pd_data['kh_key'])} temp data in file: {filepath}")

                pd_df = pd.DataFrame.from_dict(pd_data)
                pd_df.to_csv(filepath, index=False)

            proceed_temp_df = self._process_created_temp_files()

            self._write_with_versioning(proceed_temp_df, kh_key_item["kh_key"])

            self._save_proceed_kh_key(kh_key_item)

            logger.debug(f"Processing kh_key: '{kh_key_item['kh_key']}' is done!")

            # time.sleep(5)

        logger.debug(f"Retrieve distance of standorte from the kh_key and plz list done. {(time.time() - start_time)}")

    def _save_proceed_kh_key(self, kh_key_item):
        key_to_save = self._get_khkey_compare_key(kh_key_item)
        filepath = self._get_proceed_kh_key_file()
        saving_kh_key_data = {"item": [key_to_save]}
        saving_pd_df = pd.DataFrame.from_dict(saving_kh_key_data)
        if os.path.exists(filepath):
            pd_df = pd.read_csv(filepath, dtype=str)
            saving_pd_df = pd.concat([pd_df, saving_pd_df])
        saving_pd_df.to_csv(filepath, index=False)

    def _get_khkey_compare_key(self, kh_key_item):
        key_to_save = kh_key_item['kh_key'] + '_' + str(kh_key_item['latitude_kh']) + '_' + str(
            kh_key_item['longitude_kh'])
        return key_to_save

    def _get_proceed_kh_key_file(self):
        filepath = os.path.join(self.proceed_temp_path, f"proceed_kh_key.csv")
        return filepath

    def _get_new_temp_file_path(self):
        filepath = os.path.join(self.proceed_temp_path, f"distance_standort_{uuid.uuid4()}_temp.csv")
        while os.path.exists(filepath):
            filepath = os.path.join(self.proceed_temp_path, f"distance_standort_{uuid.uuid4()}_temp.csv")
        return filepath

    def _create_dataframe(self, dist_stand_list):
        dist_stand_schema = StructType([
            StructField(DistanzenStandortPlzSourceSchema.kh_key.NAME, StringType()),
            StructField(DistanzenStandortPlzSourceSchema.latitude_kh.NAME, DoubleType()),
            StructField(DistanzenStandortPlzSourceSchema.longitude_kh.NAME, DoubleType()),
            StructField(DistanzenStandortPlzSourceSchema.plz.NAME, StringType()),
            StructField(DistanzenStandortPlzSourceSchema.latitude_plz.NAME, StringType()),
            StructField(DistanzenStandortPlzSourceSchema.longitude_plz.NAME, StringType()),
            StructField(DistanzenStandortPlzSourceSchema.fahrstrecke_km.NAME, DoubleType()),
            StructField(DistanzenStandortPlzSourceSchema.fahrzeit_min.NAME, DoubleType()),
            StructField(DistanzenStandortPlzSourceSchema.relevanz.NAME, StringType())
        ])
        dist_stand_df = self.spark.createDataFrame(data=dist_stand_list, schema=dist_stand_schema)
        return dist_stand_df

    def _write_without_versioning(self, dist_stand_df):
        dist_stand_write_df = dist_stand_df.withColumnRenamed(DistanzenStandortPlzSourceSchema.fahrzeit_min.NAME,
                                                              DistanzenStandortPlzSchema.fahrzeit.NAME) \
            .withColumnRenamed(DistanzenStandortPlzSourceSchema.fahrstrecke_km.NAME,
                               DistanzenStandortPlzSchema.fahrstrecke.NAME)
        dist_stand_write_df = dist_stand_write_df.drop(DistanzenStandortPlzSourceSchema.latitude_kh.NAME,
                                                       DistanzenStandortPlzSourceSchema.longitude_kh.NAME,
                                                       DistanzenStandortPlzSourceSchema.latitude_plz.NAME,
                                                       DistanzenStandortPlzSourceSchema.longitude_plz.NAME)
        fahrzeit_col = DistanzenStandortPlzSchema.fahrzeit.COL
        dist_stand_write_df = \
            dist_stand_write_df.withColumn(DistanzenStandortPlzSchema.fahrzone.NAME,
                                           when(fahrzeit_col < 10, lit(1)).otherwise(
                                               when(fahrzeit_col < 20, lit(2)).otherwise(
                                                   when(fahrzeit_col < 30, lit(3)).otherwise(
                                                       when(fahrzeit_col < 40, lit(4)).otherwise(
                                                           when(fahrzeit_col < 50, lit(5)).otherwise(
                                                               when(fahrzeit_col < 60, lit(6)).otherwise(lit(7))
                                                           )
                                                       )
                                                   )
                                               )
                                           ))
        dist_stand_write_df.show()
        DatabaseWriter(self.tableau_table.db).write(df=dist_stand_write_df, table=self.tableau_table,
                                                    save_mode="overwrite")

    def _write_with_versioning(self, dist_stand_df: DataFrame, kh_key: str):
        dist_stand_version_df = dist_stand_df.withColumn(DistanzenStandortPlzSourceSchema.relevanz.NAME,
                                                         concat(DistanzenStandortPlzSourceSchema.kh_key.COL,
                                                                DistanzenStandortPlzSourceSchema.plz.COL))

        gueltig_ab_date = date.today().strftime("%Y%m%d")
        versioning = GenericVersioning(
            table=self.source_table,
            gueltig_ab_date=gueltig_ab_date,
            extended_logging=True
        )

        dist_stand_version_kh_key_df = \
            dist_stand_version_df.filter(DistanzenStandortPlzSourceSchema.kh_key.COL == kh_key)

        cases_to_update, cases_to_insert = versioning.calculate_upsert(data=dist_stand_version_kh_key_df,
                                                                       filter_cond=f"kh_key = '{kh_key}'")
        #cases_to_update.show()
        #cases_to_insert.show()
        db_writer = DatabaseWriter(self.source_table.db)
        #db_writer._create_partition_table_if_missing(self.source_table, partition=kh_key)
        db_writer.upsert(cases_to_update,
                         cases_to_insert,
                         table=self.source_table,
                         partition=kh_key)

    @staticmethod
    def _get_driving_distance(long_src: str, lat_src: str, long_tgt: str, lat_tgt: str):
        loc = f'{long_src},{lat_src};{long_tgt},{lat_tgt}'
        base_url = "http://router.project-osrm.org/route/v1/driving/"
        base_url = "http://localhost:5500/route/v1/driving/"
        url = f"{base_url}{loc}?skip_waypoints=true&overview=false"

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

        distance = DistanceCalculationApp._round_float(distance)
        duration = DistanceCalculationApp._round_float(duration)

        # time.sleep(1)
        return distance, duration

    @staticmethod
    def _round_float(f_value: float):
        f_value_str = "%.2f" % f_value
        f_value = float(f_value_str)
        return f_value

    @staticmethod
    def _get_plz_list():

        logger.debug(f"Retrieving the plz list from {GEOGRAFIE_BASIC_TABLE.full_db_path}")

        df = DatabaseReader(GEOGRAFIE_BASIC_TABLE.db).read(GEOGRAFIE_BASIC_TABLE, skip_schema_validation=True)
        df = df.select(GeografieBasicSchema.plz.NAME,
                       GeografieBasicSchema.longitude_plz.COL.cast("double"),
                       GeografieBasicSchema.latitude_plz.COL.cast("double")).orderBy(GeografieBasicSchema.plz.COL.asc())
        df = df.filter(GeografieBasicSchema.latitude_plz.COL.isNotNull() &
                       GeografieBasicSchema.longitude_plz.COL.isNotNull())
        df = df.distinct()

        results = df.collect()
        plz_list = [{"plz": r["plz"],
                     "longitude_plz": r["longitude_plz"],
                     "latitude_plz": r["latitude_plz"]} for r in results]
        return plz_list

    def _get_kh_key_list(self):

        logger.debug(f"Retrieving the kh_key list from {QB_KH_STAMM_SOURCE_TABLE.full_db_path}")

        df = DatabaseReader(QB_KH_STAMM_SOURCE_TABLE.db).read(QB_KH_STAMM_SOURCE_TABLE, skip_schema_validation=True)

        gueltig_bis_list = df.select(QbKhStammSourceSchema.gueltig_bis). \
            distinct(). \
            orderBy(QbKhStammSourceSchema.gueltig_bis.COL.desc()) \
            .collect()
        assert len(gueltig_bis_list) > 0, "There must be minimum one item in gueltig_bis_list"

        gueltig_bis_list = [r["gueltig_bis"] for r in gueltig_bis_list if r["gueltig_bis"] is not None]

        df = df.select(QbKhStammSourceSchema.kh_key.NAME,
                       QbKhStammSourceSchema.longitude.COL.cast("double"),
                       QbKhStammSourceSchema.latitude.COL.cast("double"),
                       QbKhStammSourceSchema.gueltig_bis.NAME).orderBy(QbKhStammSourceSchema.kh_key.COL.asc())

        df = df.filter(QbKhStammSourceSchema.longitude.COL.isNotNull() &
                       QbKhStammSourceSchema.latitude.COL.isNotNull())

        result_df = df
        if len(gueltig_bis_list) > 0:
            last_gueltig_bis = gueltig_bis_list[0]
            current_df = df.filter(QbKhStammSourceSchema.gueltig_bis.COL.isNull())
            last_df = df.filter(QbKhStammSourceSchema.gueltig_bis.COL == last_gueltig_bis)

            result_df = current_df.alias("c").join(last_df.alias("o"),
                                                   current_df[QbKhStammSourceSchema.kh_key.NAME] == last_df[
                                                       QbKhStammSourceSchema.kh_key.NAME], how="left")
            result_df = \
                result_df.filter((col("c.longitude") != col("o.longitude")) |
                                 (col("c.latitude") != col("o.latitude")) |
                                 col("o.longitude").isNull() |
                                 col("o.latitude").isNull())

        result_df = result_df.select("c.*").distinct()

        results = result_df.collect()
        kh_key_list = [{"kh_key": r["kh_key"],
                        "longitude_kh": r["longitude"],
                        "latitude_kh": r["latitude"]} for r in results]
        return kh_key_list

    def _read_shape_files(self):
        shape_file_root_path = "/mnt/daten/Prognose/2019/LOCAL(R)_Geodaten/Projektion_4283"
        shape_files = []
        for f in os.listdir(shape_file_root_path):
            if f.lower().endswith(".shp"):
                shape_files.append(os.path.join(shape_file_root_path, f))
        assert len(shape_files) > 0, f"There must be minimum one shp file in '{shape_file_root_path}'"
        shape_file_path = "/mnt/daten/Prognose/2019/LOCAL(R)_Geodaten/Projektion_4283/p19_4283.shp"  # shape_files[0]
        pandas_shape_df = geopandas.read_file(shape_file_path)
        print(pandas_shape_df.head(10))
        # shapefile_df = self.spark.createDataFrame(pandas_shape_df)
        # shapefile_df.show(30, False)
        rows = len(pandas_shape_df.index)
        ## Centre Points PLZ bestimmen
        # data frame mit PLZ erstellen
        plz_centre_points_list = []
        for i in range(0, rows):
            item = self.plz_centre_point(pandas_shape_df, i)
            plz_centre_points_list.append(item)
        for i in range(0, rows):
            item = self.get_sample_value(pandas_shape_df, i, 25)
            print(item)

    def plz_centre_point(self, pandas_shape_df, i) -> Dict:
        center_coords = list(pandas_shape_df.loc[i]["geometry"].centroid.coords)

        center_point = center_coords[0]

        return {"plz": pandas_shape_df.loc[i]["PLZ"],
                "plz_centre_longitude": center_point[0],
                "plz_centre_latitude": center_point[1]}

    def get_sample_value(self, shape_df: GeoDataFrame, plz_index: int, sample_length: int):
        pass


if __name__ == '__main__':
    DistanceCalculationApp().main(
        # reset=True
    )
