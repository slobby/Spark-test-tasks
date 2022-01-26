"""Task3."""


from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import to_timestamp, when, col
from pyspark.sql.types import IntegerType, DataType
import logging

JORNALS_SOURCE_FILE = '..\\output\\Journals\\*.csv'
RATING_SOURCE_FILE = '..\\dataset\\Ratings.csv'
SAVED_FOLDER = '..\\output\\Reports\\Report_by_'
ERRORS_FOLDER = '..\\output\\Errors'
ERROR_FIELD = '#ERROR'
RANGE = (1, 5)
JORNALS_DATE_FORMAT = 'M/d/yyyy H:mm'
JORNALS_FULL_TEXT_START_DATE = 'Full_Text_Start_Date'
JORNALS_START_DATE_INT = 'Start_Date_Int'
JORNALS_FULL_TEXT_END_DATE = 'Full_Text_End_Date'
JORNALS_END_DATE_INT = 'End_Date_Int'
RATING_DATE_FORMAT = 'M/d/yyyy H:mm'
RATING_DATE_FORMAT_2 = 'd/M/yyyy H:mm a'
RATING_DATE = 'Rating_Date'
RATING_DATE_INT = 'Rating_Date_Int'

CURIENT_TIME = datetime.now().strftime('%m/%d/%Y %H:%M')

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')


def has_field(df: DataFrame, cln: str, field: str) -> Column:
    """Checks if column`s row equal to 'field' string in the given dataframe

    Args:
        df (DataFrame): Checked dataframe
        cln (str): Column name
        field (str): Field name

    Returns:
        Column: Result Column
    """
    return df[cln] == field


def is_out_of_range(df: DataFrame, cln: str, range: tuple[int, int]) -> Column:
    """Checks if column`s row out of the given 'range' including borders

    Args:
        df (DataFrame): Checked dataframe
        cln (str): Column name
        range (tuple[int, int]): Range (start, end)

    Returns:
        Column: [description]
    """
    return (df[cln] < range[0]) | (df[cln] > range[1])


def castColumnTo(df: DataFrame, cln: str, tpe: DataType) -> DataFrame:
    """Cast column to type in the given dataframe

    Args:
        df (DataFrame): Handled dataframe
        cln (str): Column name
        tpe (DataType): Type

    Returns:
        DataFrame: New dataframe with customed column
    """
    return df.withColumn(cln, df[cln].cast(tpe()))


def save_df_by_unique_field(df: DataFrame, cln: str, sort_by: str):
    """Finds distinct rows in the given dataframe in column 'col', and saves
    rows contained founded rows in folders with unique postfixes

    Args:
        df (DataFrame): Handled dataframe
        cln (str): Column name
        sort_by (str): Sort by this column
    """
    uniques_list = [str(row[cln])
                    for row in df.select(cln).distinct().collect()]

    for item in uniques_list:
        df.select('*')  \
            .where(col(cln) == item)  \
            .sort(sort_by, ascending=False)  \
            .write.mode('overwrite')  \
            .options(header=True)  \
            .csv(SAVED_FOLDER + item)


def report_creation():
    """Join [JORNALS_SOURCE_FILE] with [RATING_SOURCE_FILE] by MID field
    Filter out invalid records:
        if 'rating' is not in the range from [RANGE]
        if there is an error ([ERROR_FIELD])
        if 'rating date' is not in range from [JORNALS_FULL_TEXT_START_DATE]
        to [JORNALS_FULL_TEXT_END_DATE] ([JORNALS_SOURCE_FILE])
    Accumulate all invalid records in a folder named ERRORS_FOLDER
    Sort records by 'rating' from highest to lowest
    Results saved locally and split by 'country'

    """

    with SparkSession \
        .builder \
        .master('local') \
        .appName('Reports creation') \
            .getOrCreate() as spark:

        spark.conf.set("spark.sql.shuffle.partitions", 50)

        # load jornals dataframe
        raw_jornals_df = spark.read.options(
            header=True).csv(JORNALS_SOURCE_FILE)
        logger.info(f'Load sources - {JORNALS_SOURCE_FILE}')

        # add extra columns 'Start_Date_Int' and 'End_Date_Int' to
        # dataframe contains datetime -> int
        jornals_df = raw_jornals_df  \
            .withColumn(JORNALS_START_DATE_INT, when(
                col(JORNALS_FULL_TEXT_START_DATE).isNull(), CURIENT_TIME)
                .otherwise(col(JORNALS_FULL_TEXT_START_DATE))) \
            .withColumn(JORNALS_START_DATE_INT,
                        to_timestamp(JORNALS_START_DATE_INT,
                                     JORNALS_DATE_FORMAT)
                        .cast('int'))  \
            .withColumn(JORNALS_END_DATE_INT, when(
                col(JORNALS_FULL_TEXT_END_DATE)
                .isNull(), CURIENT_TIME)
                .otherwise(col(JORNALS_FULL_TEXT_END_DATE)))  \
            .withColumn(JORNALS_END_DATE_INT,
                        (to_timestamp(JORNALS_END_DATE_INT,
                                      JORNALS_DATE_FORMAT).cast('int')))

        # load ratings dataframe
        raw_rating_df = spark.read.options(
            header=True).csv(RATING_SOURCE_FILE)
        logger.info(f'Load sources - {RATING_SOURCE_FILE}')

        # transform raw_rating_df by deviding on columns
        raw_rating_df_list = []
        for i in range(1, 4):
            raw_rating_df_list.append(
                raw_rating_df.select(
                    col('MID'),
                    col(f'Country{i}').alias('Country'),
                    col(f'Rating{i}').alias('Rating'),
                    col(f'Rating_Date{i}').alias('Rating_Date')))

        raw_rating1_df = raw_rating_df_list[0]
        for i in range(1, 3):
            raw_rating1_df = raw_rating1_df.union(raw_rating_df_list[i])

        # cast Column'Rating' to Int, and add extra columns 'Rating_Date_Int'
        # to dataframe contains datetime -> int
        rating2_df = castColumnTo(raw_rating1_df, 'Rating', IntegerType)
        rating3_df = rating2_df.withColumn(
            RATING_DATE_INT,
            when(to_timestamp(RATING_DATE, RATING_DATE_FORMAT).isNotNull(),
                 to_timestamp(RATING_DATE, RATING_DATE_FORMAT).cast('int'))
            .otherwise(
                to_timestamp(RATING_DATE, RATING_DATE_FORMAT_2).cast('int')))

        raw_report_df = jornals_df.join(rating3_df, 'MID')

        # select invalid records
        # get not in rating range records
        not_in_rating_range_df = raw_report_df.filter(
            is_out_of_range(raw_report_df, 'Rating', RANGE))

        # clear from not_in_rating_range_df
        raw_report1_df = raw_report_df.subtract(not_in_rating_range_df)

        # get error records
        has_an_error = raw_report_df.filter(
            has_field(raw_report_df, 'Rating_Date', ERROR_FIELD))

        # clear from has_an_error
        raw_report2_df = raw_report1_df.subtract(has_an_error)

        # get not in date range records
        not_in_date_range_df = raw_report_df.filter(
            col(RATING_DATE_INT).isNotNull() &
            ((col(RATING_DATE_INT) < col(JORNALS_START_DATE_INT)) |
             (col(RATING_DATE_INT) > col(JORNALS_END_DATE_INT))))

        # clear from not_in_date_range_df
        report_df = raw_report2_df.subtract(not_in_date_range_df).drop(
            JORNALS_START_DATE_INT, JORNALS_END_DATE_INT, RATING_DATE_INT)

        # gather all invalid records
        invalid_df = not_in_rating_range_df.union(
            has_an_error).union(not_in_date_range_df).drop(
            JORNALS_START_DATE_INT, JORNALS_END_DATE_INT, RATING_DATE_INT)

        # save all invalid records
        invalid_df.write.mode('overwrite').options(
            header=True).csv(ERRORS_FOLDER)
        logger.info(f'Save errors dataframe in - {ERRORS_FOLDER}')

        save_df_by_unique_field(report_df, 'Country', 'Rating')
        logger.info(f'Save report dataframes in - {SAVED_FOLDER}')


if __name__ == '__main__':
    report_creation()
