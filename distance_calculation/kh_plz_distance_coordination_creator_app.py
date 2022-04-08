from typing import Dict, List, Union

from loguru import logger
from pyspark.sql import DataFrame

from pyspark.sql.functions import col, count, max

from de.mediqon.apps.geografie.distance_calculation.distance_coordination_creator_base import \
    DistanceCoordinationCreatorBase as DCCreator

from de.mediqon.core.spark_app import SparkApp

from de.mediqon.etl.read.db_reader import DatabaseReader
from de.mediqon.etl.schemas.source.krankenhaus.qb_kh_geo_source_schema import QB_KH_GEO_SOURCE_TABLE, \
    QbKhGeoSourceSchema

from de.mediqon.etl.schemas.tableau_geografie.geografie_basic import GEOGRAFIE_BASIC_TABLE, GeografieBasicSchema


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

        input_kh_key_list = kwargs.get("kh_key_list", None)
        assert input_kh_key_list is not None, "A kh_key_list as list of khkey's must be set"
        assert isinstance(input_kh_key_list, List) is not None, "The kh_key_list must be list of kh_key's or empty list"

        kh_key_list = self._get_kh_key_list(input_kh_key_list)
        plz_list = self._get_plz_list()

        self._process_distance_standort_data(kh_key_list, plz_list, reset_data)

        logger.info(f"Creating Krankenhaus PLZ Coordinations files done.")

    def _process_distance_standort_data(self, kh_key_list, plz_list, reset_data):
        dc_creator = DCCreator(read_data_chunk_size=self.read_data_chunk_size,
                               proceed_saving_folder=self.proceed_temp_path,
                               if_exists="delete")
        dc_creator.process_distance_standort_data(point_list_1=kh_key_list,
                                                  point_list_2=plz_list,
                                                  delete_old_files=reset_data)

    @staticmethod
    def _get_plz_list() -> List[Dict]:

        df = KhPlzDistanceCoordinationCreatorApp.get_plz_list_dataframe()

        results = df.collect()

        plz_list = [DCCreator.create_coordination_item(key=r["plz"],
                                                       latitude=r["latitude_plz"],
                                                       longitude=r["longitude_plz"]) for r in results]

        return plz_list

    @staticmethod
    def get_plz_list_dataframe():
        logger.debug(f"Retrieving the plz list from {GEOGRAFIE_BASIC_TABLE.full_db_path}")
        df = DatabaseReader(GEOGRAFIE_BASIC_TABLE.db).read(GEOGRAFIE_BASIC_TABLE, skip_schema_validation=True)
        df = df.select(GeografieBasicSchema.plz.NAME,
                       GeografieBasicSchema.longitude_plz.COL.cast("double"),
                       GeografieBasicSchema.latitude_plz.COL.cast("double")).orderBy(GeografieBasicSchema.plz.COL.asc())
        df = df.filter(GeografieBasicSchema.latitude_plz.COL.isNotNull() &
                       GeografieBasicSchema.longitude_plz.COL.isNotNull())
        df = df.distinct()
        return df

    @staticmethod
    def get_kh_key_dataframe(input_kh_key_list: List[str]) -> DataFrame:

        logger.debug(f"Retrieving the kh_key list from {QB_KH_GEO_SOURCE_TABLE.full_db_path}")

        kh_key_read_df = DatabaseReader(QB_KH_GEO_SOURCE_TABLE.db).read(QB_KH_GEO_SOURCE_TABLE,
                                                                        skip_schema_validation=True)

        kh_key_df = kh_key_read_df.select(QbKhGeoSourceSchema.kh_key.NAME,
                                          QbKhGeoSourceSchema.longitude.COL.cast("double"),
                                          QbKhGeoSourceSchema.latitude.COL.cast("double"),
                                          QbKhGeoSourceSchema.relevanz.NAME,
                                          QbKhGeoSourceSchema.gueltig_bis.NAME).orderBy(
            QbKhGeoSourceSchema.kh_key.COL.asc())

        if len(input_kh_key_list) > 0:
            kh_key_df = kh_key_df.filter(QbKhGeoSourceSchema.kh_key.COL.isin(input_kh_key_list))

        kh_key_df = kh_key_df.filter(QbKhGeoSourceSchema.longitude.COL.isNotNull() &
                                     QbKhGeoSourceSchema.latitude.COL.isNotNull() &
                                     QbKhGeoSourceSchema.gueltig_bis.COL.isNull()).distinct()

        kh_key_max_df = \
            kh_key_read_df.filter(QbKhGeoSourceSchema.gueltig_bis.COL.isNull()). \
                groupBy(QbKhGeoSourceSchema.kh_key.NAME). \
                agg(max(QbKhGeoSourceSchema.relevanz.NAME).alias("max_relevanz"))

        condition = (kh_key_df[QbKhGeoSourceSchema.kh_key.NAME] == kh_key_max_df[QbKhGeoSourceSchema.kh_key.NAME]) & (
                    kh_key_df[QbKhGeoSourceSchema.relevanz.NAME] == kh_key_max_df["max_relevanz"])
        kh_key_joined_df = kh_key_df.alias("m").join(kh_key_max_df.alias("mx"), condition, "inner")

        kh_key_joined_df = kh_key_joined_df.select("m.*")

        return kh_key_joined_df

    @staticmethod
    def _get_kh_key_list(input_kh_key_list: List[str]) -> List[Dict]:

        kh_key_df = KhPlzDistanceCoordinationCreatorApp.get_kh_key_dataframe(input_kh_key_list)
        results = kh_key_df.collect()
        kh_key_list = [DCCreator.create_coordination_item(key=r["kh_key"],
                                                          latitude=r["latitude"],
                                                          longitude=r["longitude"]) for r in results]

        return kh_key_list


if __name__ == '__main__':
    KhPlzDistanceCoordinationCreatorApp().main(
        reset=True,
        kh_key_list=['260620157|01']
    )
