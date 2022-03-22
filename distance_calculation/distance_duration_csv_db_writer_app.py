import os
from datetime import date
from typing import Dict, List

import pandas as pd
from loguru import logger

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat, lit, when, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from de.mediqon.apps.geografie.distance_calculation.coordination_schmea import DistanceCoordinationFileHelper, \
    DistanceDurationDbTypes, DistanceDurationSchema
from de.mediqon.apps.geografie.distance_calculation.kh_plz_distance_coordination_creator_app import \
    KhPlzDistanceCoordinationCreatorApp
from de.mediqon.etl.etl_constants import EtlConstants as E

from de.mediqon.core.spark_app import SparkApp
from de.mediqon.etl.read.db_reader import DatabaseReader
from de.mediqon.etl.schemas.source.krankenhaus.qb_kh_stamm_source_schema import QB_KH_STAMM_SOURCE_TABLE, \
    QbKhStammSourceSchema as QKS_Schema
from de.mediqon.etl.schemas.source.geografie.distanzen_standort_plz_schema import DISTANZEN_STANDORT_PLZ_SOURCE_TABLE, \
    DistanzenStandortPlzSourceSchema as DSP_Schema
from de.mediqon.etl.schemas.tableau_geografie.distanzen_standort_plz import DISTANZEN_STANDORT_PLZ_TABLE, \
    DistanzenStandortPlzSchema
from de.mediqon.etl.schemas.tableau_geografie.geografie_basic import GEOGRAFIE_BASIC_TABLE, GeografieBasicSchema

from de.mediqon.etl.write.db_writer import DatabaseWriter
from de.mediqon.etl.write.generic_versioning import GenericVersioning
from de.mediqon.utils.db.db_constants import DBConstants
from de.mediqon.utils.spark.df_utils import cache_df


class DistanceDurationCsvApp(SparkApp):

    def __init__(self):
        super(DistanceDurationCsvApp, self).__init__(app_name="Write Distance Duration In DB")

    source_table = DISTANZEN_STANDORT_PLZ_SOURCE_TABLE
    tableau_table = DISTANZEN_STANDORT_PLZ_TABLE

    read_data_chunk_size = 1000

    def run_app(self, *args, **kwargs):

        logger.info(f"Writing distance and duration's from csv files in db")

        root_folder = self._get_root_folder()
        csv_file_list = self._extract_csv_files(root_folder)

        plz_df = KhPlzDistanceCoordinationCreatorApp.get_plz_list_dataframe()
        kh_key_df = KhPlzDistanceCoordinationCreatorApp.get_kh_key_dataframe()

        cache_df(plz_df)
        cache_df(kh_key_df)

        gueltig_ab_date = date.today().strftime("%Y%m%d")

        kh_key_data_list = self._extract_kh_key_data_from_files(csv_file_list)

        self._prepare_write_kh_key_data(gueltig_ab_date, kh_key_data_list, kh_key_df, plz_df)

        self._write_without_versioning()

        logger.debug(f"Writing versioned data in db is done.")

    def _prepare_write_kh_key_data(self, gueltig_ab_date, kh_key_data_list, kh_key_df, plz_df):
        total = len(kh_key_data_list.keys())
        p_index = 1
        for kh_key in kh_key_data_list:
            kh_key = '260510860|01'
            logger.debug(f"Prepare and write t5he data of kh_key '{kh_key}' in db ... ({p_index}/{total})")
            data = kh_key_data_list[kh_key]
            data = [list(i.values()) for i in data]
            dist_dur_df = self._create_dataframe(data, kh_key_df, plz_df)
            #dist_dur_df.show()

            self._write_with_versioning(dist_dur_df, kh_key, gueltig_ab_date)
            p_index += 1

    @staticmethod
    def _extract_kh_key_data_from_files(csv_file_list: List[str]) -> Dict:
        logger.debug(f"Reading distance and duration's from csv files ...")
        kh_key_data_list = {}
        for csv_file in csv_file_list:
            logger.debug(f"Reading data from {csv_file} ...")
            csv_pd_fd = pd.read_csv(csv_file, dtype=DistanceDurationDbTypes, sep=";")
            pd_data_list = csv_pd_fd.to_dict(orient='records')
            kh_key_list = [f[DistanceDurationSchema.key1] for f in pd_data_list]
            kh_key_list = set(kh_key_list)
            for kh_key in kh_key_list:
                if kh_key not in kh_key_data_list:
                    kh_key_data_list[kh_key] = []
                kh_key_items = [f for f in pd_data_list if f[DistanceDurationSchema.key1] == kh_key]
                kh_key_data_list[kh_key] += kh_key_items

            #if len(kh_key_data_list.keys()) > 3:
            #    break
        logger.debug(f"Reading distance and duration's from csv files is finish")
        logger.debug(f"There is {len(kh_key_data_list.keys())} kh_keys in {len(csv_file_list)} csv files.")
        return kh_key_data_list

    @staticmethod
    def _get_root_folder():
        root_folder = f"{E.distance_duration_csv_root_path}"

        if not os.path.exists(root_folder):
            raise Exception(f"There is no folder named '{root_folder}'")

        return root_folder

    @staticmethod
    def _extract_csv_files(root_folder: str) -> [List[str], List[str]]:

        files = os.listdir(root_folder)
        data_files = [os.path.join(root_folder, f) for f in files if
                      f.startswith(DistanceCoordinationFileHelper.result_file_prefix)
                      and f.lower().endswith(".csv")]

        if len(data_files) == 0:
            raise Exception(f"There is {len(data_files)} items in {root_folder}.")

        logger.debug(f"There is {len(data_files)} items in {root_folder}.")
        data_files.sort()
        return data_files

    def _create_dataframe(self, dist_stand_list, kh_key_df: DataFrame, plz_df: DataFrame):
        dist_stand_schema = StructType([
            StructField(DSP_Schema.kh_key.NAME, StringType()),
            StructField(DSP_Schema.plz.NAME, StringType()),
            StructField(DSP_Schema.fahrstrecke_km.NAME, DoubleType()),
            StructField(DSP_Schema.fahrzeit_min.NAME, DoubleType())
        ])
        df = self.spark.createDataFrame(data=dist_stand_list, schema=dist_stand_schema)

        join_df = df.alias("d").join(kh_key_df.alias("k"),
                                     df[DSP_Schema.kh_key.NAME] == kh_key_df[QKS_Schema.kh_key.NAME],
                                     "left")

        join_df = join_df.select("d.*",
                                 QKS_Schema.longitude.COL.alias(DSP_Schema.longitude_kh.NAME),
                                 QKS_Schema.latitude.COL.alias(DSP_Schema.latitude_kh.NAME))

        self._verfy_unknown_data(join_df, DSP_Schema.longitude_kh.NAME)

        dist_stand_df = join_df.alias("d").join(plz_df,
                                                join_df[DSP_Schema.plz.NAME] == plz_df[
                                                    GeografieBasicSchema.plz.NAME],
                                                "left")

        dist_stand_df = dist_stand_df.select("d.*",
                                             GeografieBasicSchema.longitude_plz.NAME,
                                             GeografieBasicSchema.latitude_plz.NAME)

        self._verfy_unknown_data(dist_stand_df, GeografieBasicSchema.longitude_plz.NAME)

        return dist_stand_df

    @staticmethod
    def _verfy_unknown_data(join_df: DataFrame, column: str):
        test_df = join_df.filter(col(column).isNull())
        if test_df.count() > 0:
            unknown_list = test_df.select(DSP_Schema.kh_key.NAME).distinct().collect()
            unknown_list = [r[DSP_Schema.kh_key.NAME] for r in unknown_list]
            raise Exception(f"There is unknown kh_key in csv data: {unknown_list}")

    def _write_without_versioning(self):
        logger.debug(f"Read data from {self.source_table.full_db_path} as source and "
                     f"write in {self.tableau_table.full_db_path} as tableau table")

        sql = f"SELECT distinct kh_key FROM {self.source_table.name_with_schema} " \
              f"where {DSP_Schema.gueltig_bis.NAME} is null order by kh_key;"

        kh_key_list = DBConstants().db_connector.execute_sql(db=self.source_table.db, sql=sql, do_fetch="all")
        kh_key_list = [r[0] for r in kh_key_list]

        for kh_key in kh_key_list:
            source_data_df = DatabaseReader(self.source_table.db).read(table=self.source_table, partition=kh_key)
            source_data_df = source_data_df.filter(DSP_Schema.gueltig_bis.COL.isNull())

            target_data_df = source_data_df.select(DSP_Schema.kh_key.NAME,
                                                   DSP_Schema.plz.NAME,
                                                   DSP_Schema.fahrstrecke_km.COL.alias(
                                                       DistanzenStandortPlzSchema.fahrstrecke.NAME),
                                                   DSP_Schema.fahrzeit_min.COL.alias(
                                                       DistanzenStandortPlzSchema.fahrzeit.NAME))

            fahrzeit_col = DistanzenStandortPlzSchema.fahrzeit.COL

            target_data_df = target_data_df.withColumn(DistanzenStandortPlzSchema.fahrzone.NAME,
                                                       when(fahrzeit_col < 10, lit(1)).otherwise(
                                                           when(fahrzeit_col < 20, lit(2)).otherwise(
                                                               when(fahrzeit_col < 30, lit(3)).otherwise(
                                                                   when(fahrzeit_col < 40, lit(4)).otherwise(
                                                                       when(fahrzeit_col < 50, lit(5)).otherwise(
                                                                           when(fahrzeit_col < 60,
                                                                                lit(6)).otherwise(lit(7))
                                                                       )
                                                                   )
                                                               )
                                                           )
                                                       ))

            target_data_df.show()
            DatabaseWriter(self.tableau_table.db).write(target_data_df,
                                                        table=self.tableau_table,
                                                        save_mode="overwrite",
                                                        partition=kh_key)

    def _write_with_versioning(self, dist_dur_df: DataFrame, kh_key: str, gueltig_ab_date: str):
        dist_dur_version_df = dist_dur_df.withColumn(DSP_Schema.relevanz.NAME,
                                                     concat(DSP_Schema.kh_key.COL, DSP_Schema.plz.COL))

        versioning = GenericVersioning(
            table=self.source_table,
            gueltig_ab_date=gueltig_ab_date,
            extended_logging=True
        )

        dist_stand_version_kh_key_df = \
            dist_dur_version_df.filter(DSP_Schema.kh_key.COL == kh_key)

        cases_to_update, cases_to_insert = versioning.calculate_upsert(data=dist_stand_version_kh_key_df,
                                                                       filter_cond=f"kh_key = '{kh_key}'")
        # cases_to_update.show()
        # cases_to_insert.show()
        db_writer = DatabaseWriter(self.source_table.db)
        db_writer.upsert(cases_to_update, cases_to_insert, table=self.source_table, partition=kh_key)


if __name__ == '__main__':
    DistanceDurationCsvApp().main()
