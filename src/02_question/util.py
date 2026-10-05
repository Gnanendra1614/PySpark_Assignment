from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


# Mask card number and show only last 4 digits
def mask_card_number(card_number):

    if card_number is None:
        return None

    return "*" * (len(card_number) - 4) + card_number[-4:]


# Register function as UDF
mask_card_udf = udf(
    mask_card_number,
    StringType()
)