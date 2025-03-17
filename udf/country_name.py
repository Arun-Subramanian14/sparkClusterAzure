import pycountry
from fuzzywuzzy import process

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def correct_country_name(name, threshold=85):
    countries=[country.name for country in pycountry.countries]
    corrected_name, score=process.extractOne(name,countries)
    if score>=threshold:
        return corrected_name
    return name

def corrected_country_name_df(df):
    correct_country_name_udf= udf(correct_country_name, StringType())
    df = df.withColumn('country', correct_country_name_udf(df['country']))
    return df
