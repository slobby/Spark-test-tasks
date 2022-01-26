"""Task2."""


from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import logging

LSD_SOURCE_FILE = '..\\output\\LSD-titles\\*.csv'
MFS_SOURCE_FILE = '..\\dataset\\MFS-publist.csv'
SAVED_FOLDER = '..\\output\\Journals'

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')


def journals_creation():
    """
    Join [LSD_SOURCE_FILE] and [MFS_SOURCE_FILE] by ISSN
    If ISSN is empty, use “title” field
    Save joined dataset locally in a folder named [SAVED_FOLDER]
"""

    with SparkSession \
        .builder \
        .master('local') \
        .appName('Journals creation') \
            .getOrCreate() as spark:
        lsd_df = spark.read.options(header=True).csv(LSD_SOURCE_FILE)
        logger.info(f'Load sources - {LSD_SOURCE_FILE}')
        mfs_df = spark.read.options(header=True).csv(MFS_SOURCE_FILE)
        logger.info(f'Load sources - {MFS_SOURCE_FILE}')
        lsd_df.createOrReplaceTempView("lsd")
        mfs_df.createOrReplaceTempView("mfs")

        jornal_df = spark.sql(
            """
            select lsd.NLMUID,
                   lsd.Normalized_Title AS Medline_Title,
                   lsd.Medline_Title_Abbreviation,
                   CASE
                        WHEN lsd.ISSN NOT NULL THEN lsd.ISSN
                        WHEN mfs.ISSN NOT NULL THEN mfs.ISSN
                        ELSE ''
                   END AS ISSN,
                   CASE
                        WHEN lsd.eISSN NOT NULL THEN lsd.eISSN
                        WHEN mfs.eISSN NOT NULL THEN mfs.eISSN
                        ELSE ''
                   END AS eISSN,
                   lsd.Full_Text_Start_Date,
                   lsd.Full_Text_End_Date,
                   mfs.MID,
                   mfs.PUBLISHER
            from lsd
            INNER JOIN mfs
            ON (lsd.ISSN NOT NULL
            AND mfs.ISSN NOT NULL
            AND lsd.ISSN == mfs.ISSN)
            OR lsd.Normalized_Title == mfs.MAGNAME
            """
        )

        jornal_df.write.mode('overwrite').options(
            header=True).csv(SAVED_FOLDER)
        logger.info(f'Save dataframe in - {SAVED_FOLDER}')


if __name__ == '__main__':
    journals_creation()
