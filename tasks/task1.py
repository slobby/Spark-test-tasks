"""Task1."""


from statistics import mode
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import logging

SOURCE_FILE = '..\\dataset\\LSD-titles.csv'
SAVED_FOLDER = '..\\output\\LSD-Titles'
SOURCE_FIELD = 'Medline_Title'
NEW_FIELD = 'Normalized_Title'
table_changes = {' and ': ' & ',
                 'A ': '',
                 'The ': ''}

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')


@udf(StringType())
def replacer(field: str) -> StringType():
    """Replace words and phrases

    Args:
        field (str): inner 

    Returns:
        [type]: Column
    """
    for key, val in table_changes.items():
        if key in field:
            field = field.replace(key, val)
    return field


def normalize_title():
    """
    Replace “and“ with “&“
    Remove any leading articles such as “A “ or “The“
    Create a new field called “Normalized_Title”
    Save all transformations in “Normalized_Title” field
    “Medline_Title” field should be kept without changes
    Save normalized dataset on S3 in a folder named “LSD-Titles”
    """
    with SparkSession \
        .builder \
        .master('local') \
        .appName('Normalize titles') \
            .getOrCreate() as spark:
        df = spark.read.options(header=True).csv(SOURCE_FILE)
        logger.info(f'Load source - {SOURCE_FILE}')

        cdf = df.withColumn(NEW_FIELD, replacer(SOURCE_FIELD))
        logger.info(f'Handle datafarme - {cdf}')

        cdf.write.mode('overwrite').options(header=True).csv(SAVED_FOLDER)
        logger.info(f'Save dataframe in - {SAVED_FOLDER}')


if __name__ == '__main__':
    normalize_title()
