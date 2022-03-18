import json
import os
from typing import Dict, List

import pandas as pd
from loguru import logger

from pyspark.sql.functions import col, concat, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType

from de.mediqon.apps.geografie.distance_calculation.distance_coordination_creator_base import \
    DistanceCoordinationCreatorBase as DCCreator
from de.mediqon.core.spark_app import SparkApp

from de.mediqon.etl.read.db_reader import DatabaseReader
from de.mediqon.etl.schemas.source.krankenhaus.qb_kh_stamm_source_schema import QB_KH_STAMM_SOURCE_TABLE, \
    QbKhStammSourceSchema

from de.mediqon.etl.schemas.tableau_geografie.geografie_basic import GEOGRAFIE_BASIC_TABLE, GeografieBasicSchema
import time


class KhPlzDistanceCoordinationCreatorApp(SparkApp):

    def __init__(self):
        super(KhPlzDistanceCoordinationCreatorApp, self).__init__(app_name="KH-PLZ Coordination Creator")

    proceed_temp_path = "/mnt/daten/distance_place_temp"
    read_data_chunk_size = 500000

    def run_app(self, *args, **kwargs):

        logger.info(f"Creating Krankenhaus PLZ Coordinations files ...")

        reset_data = kwargs.get("reset", None)
        if reset_data is not None:
            if reset_data or (str(reset_data).lower() == "true"):
                reset_data = True
            else:
                reset_data = False
        else:
            reset_data = False

        kh_key_list = self._get_kh_key_list()
        plz_list = self._get_plz_list()

        dc_creator = DCCreator(read_data_chunk_size=self.read_data_chunk_size,
                               proceed_saving_folder=self.proceed_temp_path,
                               if_exists="delete")
        dc_creator.process_distance_standort_data(point_list_1=kh_key_list,
                                                  point_list_2=plz_list,
                                                  delete_old_files=reset_data)

        logger.info(f"Creating Krankenhaus PLZ Coordinations files done.")

    @staticmethod
    def _get_plz_list() -> List[Dict]:

        logger.debug(f"Retrieving the plz list from {GEOGRAFIE_BASIC_TABLE.full_db_path}")

        df = DatabaseReader(GEOGRAFIE_BASIC_TABLE.db).read(GEOGRAFIE_BASIC_TABLE, skip_schema_validation=True)
        df = df.select(GeografieBasicSchema.plz.NAME,
                       GeografieBasicSchema.longitude_plz.COL.cast("double"),
                       GeografieBasicSchema.latitude_plz.COL.cast("double")).orderBy(GeografieBasicSchema.plz.COL.asc())
        df = df.filter(GeografieBasicSchema.latitude_plz.COL.isNotNull() &
                       GeografieBasicSchema.longitude_plz.COL.isNotNull())
        df = df.distinct()

        results = df.collect()

        plz_list = [DCCreator.create_coordination_item(key=r["plz"],
                                                       latitude=r["longitude_plz"],
                                                       longitude=r["latitude_plz"]) for r in results]

        return plz_list

    @staticmethod
    def _get_kh_key_list() -> List[Dict]:

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
        kh_key_list = [DCCreator.create_coordination_item(key=r["kh_key"],
                                                          latitude=r["latitude"],
                                                          longitude=r["longitude"]) for r in results]
        return kh_key_list


if __name__ == '__main__':
    KhPlzDistanceCoordinationCreatorApp().main(
        reset=True
    )
