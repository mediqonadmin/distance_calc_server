import os
from datetime import date
from typing import Dict, List

import pandas as pd
from loguru import logger

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat, lit, when, count

from de.mediqon.core.spark_app import SparkApp
from de.mediqon.etl.read.db_reader import DatabaseReader
from de.mediqon.etl.read.query_reader import QueryReader
from de.mediqon.etl.schemas.source.geografie.distanzen_standort_plz_schema import DISTANZEN_STANDORT_PLZ_SOURCE_TABLE, \
    DistanzenStandortPlzSourceSchema as DSP_Schema
from de.mediqon.etl.schemas.tableau_geografie.distanzen_standort_plz_schema import DISTANZEN_STANDORT_PLZ_TABLE, \
    DistanzenStandortPlzSchema
from de.mediqon.etl.source_kh_key_list import SourceKhKeys

from de.mediqon.etl.write.db_writer import DatabaseWriter


class DistanceDurationTableauDbWriterApp(SparkApp):

    def __init__(self):
        super(DistanceDurationTableauDbWriterApp, self).__init__(app_name="Write Distance Duration In Tableau DB")

    source_table = DISTANZEN_STANDORT_PLZ_SOURCE_TABLE
    tableau_table = DISTANZEN_STANDORT_PLZ_TABLE

    read_data_chunk_size = 1000

    def run_app(self, *args, **kwargs):

        logger.info(f"Writing distance and duration's from {self.source_table.full_db_path} in {self.tableau_table.full_db_path}")

        input_kh_key_list = kwargs.get("kh_key_list", None)
        assert input_kh_key_list is not None, "A kh_key_list as list of khkey's must be set"
        assert isinstance(input_kh_key_list, List), "The kh_key_list must be list of kh_key's or empty list"

        self._write_in_tableau_db(input_kh_key_list)

        self.cleanup_cached_dataframes()

        logger.debug(f"Writing versioned data in db is done.")

    def _write_in_tableau_db(self, input_kh_key_list: List[str]):
        logger.debug(f"Read data from {self.source_table.full_db_path} as source and "
                     f"write in {self.tableau_table.full_db_path} as tableau table")

        kh_key_list = self._extract_kh_key_list(input_kh_key_list)

        totla_count = len(kh_key_list)

        w_index = 1
        for kh_key in kh_key_list:
            logger.debug(f"Writing kh_key '{kh_key}' from '{self.source_table.full_db_path}' in '{self.tableau_table.full_db_path}' ({w_index} / {totla_count})")
            gueltig_ab = kh_key_list[kh_key]

            if kh_key not in SourceKhKeys().all_kh_key_partition_values:
                logger.warning(f"The kh_key '{kh_key}' from '{self.source_table.full_db_path}' "
                               f"does not exists in '{self.tableau_table.full_db_path}'")
                continue

            target_data_df = self._prepare_target_data(gueltig_ab, kh_key)

            self._write_target_data(kh_key, target_data_df)

            w_index += 1

    def _write_target_data(self, kh_key, target_data_df):
        DatabaseWriter(self.tableau_table.db).write(target_data_df,
                                                    table=self.tableau_table,
                                                    save_mode="overwrite",
                                                    partition=kh_key)

    def _prepare_target_data(self, gueltig_ab, kh_key):
        source_data_df = DatabaseReader(self.source_table.db).read(table=self.source_table, partition=kh_key)
        source_data_df = source_data_df.filter(DSP_Schema.gueltig_bis.COL.isNull() &
                                               (DSP_Schema.gueltig_ab.COL == gueltig_ab))
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
        return target_data_df

    def _extract_kh_key_list(self, input_kh_key_list) -> Dict:
        khkey_filter_sql = ""
        if len(input_kh_key_list) > 0:
            khkey_list_str = f"'{input_kh_key_list[0]}'"
            if len(input_kh_key_list) > 1:
                khkey_list_str = "','".join(input_kh_key_list)
                khkey_list_str = f"'{khkey_list_str}'"
            khkey_filter_sql = f" and {DSP_Schema.kh_key.NAME} in ({khkey_list_str}) "
        sql_query = f"SELECT {DSP_Schema.kh_key.NAME}, max({DSP_Schema.gueltig_ab.NAME}) as max_gueltig_ab " \
                    f"FROM {self.source_table.name_with_schema} " \
                    f"where {DSP_Schema.gueltig_bis.NAME} is null {khkey_filter_sql} " \
                    f"group by {DSP_Schema.kh_key.NAME} " \
                    f"order by {DSP_Schema.kh_key.NAME}"
        df = QueryReader(self.source_table.db).read(sql_query=sql_query)
        kh_key_list = df.collect()

        kh_key_list = {r[0]: r[1] for r in kh_key_list}
        return kh_key_list


if __name__ == '__main__':
    DistanceDurationTableauDbWriterApp().main(kh_key_list=['260620157|01'])
