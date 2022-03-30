import os
from datetime import date
from typing import Dict, List, Set

import pandas as pd
from loguru import logger

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat, lit, when, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from de.mediqon.apps.geografie.distance_calculation.coordination_schmea import DistanceCoordinationFileHelper, \
    DistanceDurationDbTypes, DistanceDurationSchema
from de.mediqon.apps.geografie.distance_calculation.kh_plz_distance_coordination_creator_app import \
    KhPlzDistanceCoordinationCreatorApp
from de.mediqon.etl.etl_constants import EtlConstants as E

from de.mediqon.core.spark_app import SparkApp
from de.mediqon.etl.read.db_reader import DatabaseReader
from de.mediqon.etl.read.query_reader import QueryReader
from de.mediqon.etl.schemas.source.geografie.kh_key_ohne_distanzen_schema import KhKeyOhneDistanzenSchema, \
    KH_KEY_OHNE_DISTANZEN_TABLE
from de.mediqon.etl.schemas.source.krankenhaus.qb_kh_stamm_source_schema import QbKhStammSourceSchema as QKS_Schema
from de.mediqon.etl.schemas.source.geografie.distanzen_standort_plz_schema import DISTANZEN_STANDORT_PLZ_SOURCE_TABLE, \
    DistanzenStandortPlzSourceSchema as DSP_Schema
from de.mediqon.etl.schemas.tableau_geografie.geografie_basic import GeografieBasicSchema

from de.mediqon.etl.write.db_writer import DatabaseWriter
from de.mediqon.etl.write.generic_versioning import GenericVersioning
from de.mediqon.utils.db.db_constants import DBConstants
from de.mediqon.utils.spark.df_utils import cache_df


class DistanceDurationCsvDbWriterApp(SparkApp):

    def __init__(self):
        super(DistanceDurationCsvDbWriterApp, self).__init__(app_name="Write Distance Duration from CSV In Source DB")

    source_table = DISTANZEN_STANDORT_PLZ_SOURCE_TABLE

    read_data_chunk_size = 1000

    def run_app(self, *args, **kwargs):

        logger.info(f"Writing distance and duration's from csv files in db")

        gueltig_ab_date = kwargs.get("gueltig_ab_date", None)
        if gueltig_ab_date is None:
            raise Exception("There is no gueltig_ab_date in argument's.")

        gueltig_ab_date = str(gueltig_ab_date)
        #gueltig_ab_date = date.today().strftime("%Y%m%d")

        gueltig_ab_today = f"{gueltig_ab_date[:4]}-{gueltig_ab_date[4:6]}-{gueltig_ab_date[6:8]}" #date.today().strftime("%Y-%m-%d")

        root_folder = self._get_root_folder()
        csv_file_list = self._extract_csv_files(root_folder)

        all_kh_key_list = self._extract_all_kh_key_list(csv_file_list)

        self._process_invalid_kh_key_items(all_kh_key_list, gueltig_ab_today)

        proceed_kh_key_list = self._extract_proceed_kh_key_list(gueltig_ab_today)

        plz_df = KhPlzDistanceCoordinationCreatorApp.get_plz_list_dataframe()
        kh_key_df = KhPlzDistanceCoordinationCreatorApp.get_kh_key_dataframe()

        cache_df(plz_df)
        cache_df(kh_key_df)

        kh_key_data_list = self._extract_kh_key_data_from_files(csv_file_list, proceed_kh_key_list)
        plz_count = plz_df.count()

        unclear_kh_list = []
        for kh_key in kh_key_data_list:
            lst = kh_key_data_list[kh_key]
            if len(lst) != plz_count:
                unclear_kh_list.append(kh_key)

        if len(unclear_kh_list) > 0:
            raise Exception(f"The distance and duration data for these kh_keys "
                            f"does not cover all PLZ list:\n {unclear_kh_list}")

        self._prepare_write_kh_key_data(gueltig_ab_date, kh_key_data_list, kh_key_df, plz_df, proceed_kh_key_list)

        self.cleanup_cached_dataframes()

        logger.debug(f"Writing versioned data in db is done.")

    def _process_invalid_kh_key_items(self, all_kh_key_list: List[str], gueltig_ab_today: str):
        logger.debug(f"Processing the list of kh_key's without distance in {self.source_table.full_db_path}")

        exists_kh_key_filter = "', '".join(all_kh_key_list)
        exists_kh_key_filter = f"'{exists_kh_key_filter}'"
        sql = f"select distinct {QKS_Schema.kh_key.NAME},{QKS_Schema.gueltig_ab.NAME},{QKS_Schema.gueltig_bis.NAME} " \
              f"FROM {self.source_table.name_with_schema} " \
              f"where {QKS_Schema.gueltig_bis.NAME} is null " \
              f"and {QKS_Schema.gueltig_ab.NAME} != '{gueltig_ab_today}' " \
              f"and {QKS_Schema.kh_key.NAME} not in ({exists_kh_key_filter}) " \
              f"order by kh_key"
        df = QueryReader(self.source_table.db).read(sql_query=sql)
        if df.count() == 0:
            logger.debug(f"There is no kh_key's without distance in database")
            return

        self._write_invalid_kh_keys_in_db(df, gueltig_ab_today)

        not_valid_items = df.collect()
        for not_valid_item in not_valid_items:
            kh_key = not_valid_item[QKS_Schema.kh_key.NAME]
            gueltig_ab = not_valid_item[QKS_Schema.gueltig_ab.NAME]
            gueltig_bis = not_valid_item[QKS_Schema.gueltig_bis.NAME]

            update_sql = f"update {self.source_table.name_with_schema} " \
                         f"set {QKS_Schema.gueltig_bis.NAME} = '{gueltig_ab_today}' " \
                         f"where {QKS_Schema.kh_key.NAME}='{kh_key}' and {QKS_Schema.gueltig_ab.NAME}='{gueltig_ab}'"
            DBConstants().db_connector.execute_sql(db=self.source_table.db, sql=update_sql)

        logger.debug(f"{len(not_valid_items)} kh_key's without distance is updated to in database")

    @staticmethod
    def _write_invalid_kh_keys_in_db(df: DataFrame, gueltig_ab_today: str):
        logger.debug(f"Write the list of kh_key's without distance in database")

        kh_key_items_df = df.select(QKS_Schema.kh_key.NAME).distinct()
        kh_key_items_df = kh_key_items_df.withColumn(KhKeyOhneDistanzenSchema.gueltig_ab.NAME,
                                                     lit(gueltig_ab_today).cast("timestamp"))
        DatabaseWriter(KH_KEY_OHNE_DISTANZEN_TABLE.db).write(df=kh_key_items_df,
                                                             table=KH_KEY_OHNE_DISTANZEN_TABLE,
                                                             save_mode="overwrite")
        logger.debug(f"{kh_key_items_df.count()} kh_key's without distance is written in {KH_KEY_OHNE_DISTANZEN_TABLE.full_db_path}")

    @staticmethod
    def _extract_all_kh_key_list(csv_file_list: List[str]) -> List[str]:
        logger.debug(f"Extracting all kh_key list from csv files ...")
        all_kh_key_list = []
        for csv_file in csv_file_list:
            csv_pd_df = pd.read_csv(csv_file, dtype=DistanceDurationDbTypes, sep=";")
            kh_key_item = list(csv_pd_df[DistanceDurationSchema.key1].unique())
            all_kh_key_list += kh_key_item
        all_kh_key_list = list(set(all_kh_key_list))
        return all_kh_key_list

    def _extract_proceed_kh_key_list(self, gueltig_ab_today):
        sql_query = f"SELECT distinct {DSP_Schema.kh_key} FROM {self.source_table.name_with_schema} where {DSP_Schema.gueltig_ab} = '{gueltig_ab_today}' and {DSP_Schema.gueltig_bis} is null"
        df = QueryReader(self.source_table.db).read(sql_query=sql_query)

        rows = df.collect()
        proceed_kh_key_list = [r[0] for r in rows]
        logger.debug(f"{len(proceed_kh_key_list)} kh_kex's are proceed.")

        return proceed_kh_key_list

    def _prepare_write_kh_key_data(self,
                                   gueltig_ab_date: str,
                                   kh_key_data_list: Dict,
                                   kh_key_df: DataFrame,
                                   plz_df: DataFrame,
                                   proceed_kh_key_list: List[str]):
        plz_src_list = plz_df.collect()
        plz_src_list = {p[GeografieBasicSchema.plz.NAME]: p for p in plz_src_list}

        total = len(kh_key_data_list.keys())
        p_index = 1

        for kh_key in kh_key_data_list:
            logger.debug(f"Prepare and write the data of kh_key '{kh_key}' in db ... ({p_index}/{total})")

            kh_key_data = kh_key_df.filter(col("kh_key") == kh_key).collect()[0]

            data_list = kh_key_data_list[kh_key]
            data_list = [list(i.values()) for i in data_list]
            for i in range(0, len(data_list)):
                data_list[i].append(kh_key_data[QKS_Schema.latitude.NAME])
                data_list[i].append(kh_key_data[QKS_Schema.longitude.NAME])
                plz = data_list[i][1]
                plz_src_item = plz_src_list[plz]
                data_list[i].append(plz_src_item[GeografieBasicSchema.latitude_plz.NAME])
                data_list[i].append(plz_src_item[GeografieBasicSchema.longitude_plz.NAME])
                data_list[i].append(data_list[i][0] + data_list[i][1])

            dist_dur_df = self._create_dataframe(data_list)
            #dist_dur_df.show()

            test_df = dist_dur_df.groupBy("kh_key", "plz").agg(count("*").alias("count")).orderBy(col("count").desc())
            test_df_count = test_df.count()
            if test_df_count != plz_df.count():
                logger.error(f"The kh_key: {kh_key} hast {test_df_count} but must have {plz_df.count()}")

            self._write_with_versioning(dist_dur_df, kh_key, gueltig_ab_date)
            p_index += 1

    @staticmethod
    def _extract_kh_key_data_from_files(csv_file_list: List[str], proceed_kh_key_list: List[str]) -> Dict:
        logger.debug(f"Reading distance and duration's from csv files ...")
        kh_key_data_list = {}
        for csv_file in csv_file_list:
            logger.debug(f"Reading data from {csv_file} ...")
            csv_pd_df = pd.read_csv(csv_file, dtype=DistanceDurationDbTypes, sep=";")
            if len(proceed_kh_key_list) > 0:
                csv_pd_df = csv_pd_df[~csv_pd_df[DistanceDurationSchema.key1].isin(proceed_kh_key_list)]
            pd_data_list = csv_pd_df.to_dict(orient='records')
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
        logger.debug(f"There is {len(kh_key_data_list.keys())} not proceed kh_key's in {len(csv_file_list)} csv files.")
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

    def _create_dataframe(self, data_list):
        dist_stand_schema = StructType([
            StructField(DSP_Schema.kh_key.NAME, StringType()),
            StructField(DSP_Schema.plz.NAME, StringType()),
            StructField(DSP_Schema.fahrstrecke_km.NAME, DoubleType()),
            StructField(DSP_Schema.fahrzeit_min.NAME, DoubleType()),
            StructField(DSP_Schema.latitude_kh.NAME, DoubleType()),
            StructField(DSP_Schema.longitude_kh.NAME, DoubleType()),
            StructField(DSP_Schema.latitude_plz.NAME, DoubleType()),
            StructField(DSP_Schema.longitude_plz.NAME, DoubleType()),
            StructField(DSP_Schema.relevanz.NAME, StringType())
        ])
        df = self.spark.createDataFrame(data=data_list, schema=dist_stand_schema)

        return df

    def _write_with_versioning(self, dist_dur_df: DataFrame, kh_key: str, gueltig_ab_date: str):

        versioning = GenericVersioning(
            table=self.source_table,
            gueltig_ab_date=gueltig_ab_date,
            extended_logging=True,
            partition=kh_key
        )

        cases_to_update, cases_to_insert = versioning.calculate_upsert(data=dist_dur_df,
                                                                       filter_cond=f"kh_key = '{kh_key}'")
        # cases_to_update.show()
        # cases_to_insert.show()
        db_writer = DatabaseWriter(self.source_table.db)
        db_writer.upsert(cases_to_update, cases_to_insert, table=self.source_table, partition=kh_key)


if __name__ == '__main__':
    DistanceDurationCsvDbWriterApp().main(gueltig_ab_date="20220323")
