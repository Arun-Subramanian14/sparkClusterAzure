import plotly.express as px
import pycountry
from fuzzywuzzy import process
import pycountry_convert as pcc
from pycountry_convert import convert_continent_code_to_continent_name

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

spark=SparkSession.builder.appName("End to end processing").getOrCreate()
df = spark.read.csv("inputs/visa_number_in_japan.csv", header=True,inferSchema=True)
new_col_names=[col_name.lstrip().replace(" ","_").replace(",","").replace(".","").replace("/","").replace("-","")
               for col_name in df.columns]
df=df.toDF(*new_col_names)

df.dropna(how='all')
df=df.select('year','country','Number_of_issued_numerical')

def correct_country_name(name, threshold=85):
    countries=[country.name for country in pycountry.countries]
    corrected_name, score=process.extractOne(name,countries)
    if score>=threshold:
        return corrected_name
    return name
correct_country_name_udf= udf(correct_country_name, StringType())
df = df.withColumn('country', correct_country_name_udf(df['country']))

def get_continent_name(country_name):
    try:
        country_code=pcc.country_name_to_country_alpha2(country_name, cn_name_format='default')
        continent_code=pcc.country_alpha2_to_continent_code(country_code)
        return convert_continent_code_to_continent_name(continent_code)
    except:
        return None


country_corrections = {
    'Andra': 'Russia',
    'Brush': 'Bhutan',
    'Barrane': 'Bahrain',
    'Gaiana': 'Guyana'
}

df = df.replace(country_corrections,subset='country')
continent_udf=udf(get_continent_name,StringType())
df = df.withColumn('continent', continent_udf(df['country']))
df.show(5)

#VISUALISATION 1
df.createGlobalTempView("japan_visa")
df_cont=spark.sql("""
    SELECT year,continent, sum(Number_of_issued_numerical) visa_issued
    from global_temp.japan_visa
    where continent is not null
    group by year, continent
    order by year
""")
df_cont.show()
df_cont= df_cont.toPandas()

fig=px.bar(df_cont,x='year',y='visa_issued',color='continent')
fig.update_layout( title_text='Number of visa issued in Japan by continent between 2006 and 2020',
           xaxis_title='Year',yaxis_title='Number of visa',
           legend_title='Continent')
fig.write_html('outputs/visa_number_in_japan_continent.html')
# fig.write_parquet('output/visa_number_in_japan_continent.parquet')


#VISUALISATION 2
df_cont1=spark.sql("""
    SELECT year,country, sum(Number_of_issued_numerical) visa_issued
    from global_temp.japan_visa
    where country not in ('total','others')
    and country is not null
    group by year, country
    order by year
""")
df_cont1=df_cont1.toPandas()

fig=px.choropleth(df_cont1,locations='country',color='visa_issued',hover_name='country', animation_frame='year'
                  ,range_color=[100000,100000],color_continuous_scale=px.colors.sequential.Plasma,
                  locationmode='country names',
                  title='Yearly visa issued by countries')

fig.write_html('outputs/visa_number_in_japan_year_map_country_wise.html')
df.write.csv('outputs/visa_number_in_japan_cleaned.csv', header=True,mode='overwrite')

spark.stop()






