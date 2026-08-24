from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
from datetime import datetime

master_data_base_path = '/user/master/data/'
my_data_base_path = '/user/s25321830/data/'
my_raw_layer_path = f'{my_data_base_path}/raw'
my_trusted_layer_path = f'{my_data_base_path}/trusted'
my_analytics_layer_path = f'{my_data_base_path}/analytics'
geo_csv_path = '/user/s25321830/geo/geo.csv'

spark = SparkSession.builder \
                    .appName('app_1_temp_name') \
                    .master('local') \
                    .getOrCreate()

events_with_coordinates_original = spark.read.parquet(f'{master_data_base_path}/geo/events')

events_with_coordinates_original.write \
                                .format('parquet') \
                                .mode('overwrite') \
                                .save(my_raw_layer_path)
events_with_coordinates_my_copy = spark.read.parquet(my_raw_layer_path)

geo_csv_as_df = spark.read.csv(path = geo_csv_path, sep=";", inferSchema=True, header=True)

geo_csv_as_df.printSchema()

geo_csv_as_df.write \
             .format('parquet') \
             .mode('overwrite') \
             .save(my_raw_layer_path)

geo_csv_my_copy = spark.read.parquet(my_raw_layer_path)

geo_csv_my_copy = geo_csv_my_copy.withColumnRenamed('lat', 'city_lat') \
                                 .withColumnRenamed('lon', 'city_lon')

enumeration = events_with_coordinates_my_copy.join(geo_csv_my_copy, on = [], how = 'cross')

enumeration_with_distance = enumeration.withColumn(
    'distance',
    F.lit(6371) * F.acos(
        F.sin(F.radians(F.col("lat"))) * F.sin(F.radians(F.col("city_lat"))) +
        F.cos(F.radians(F.col("lat"))) * F.cos(F.radians(F.col("city_lat"))) *
        F.cos(F.radians(F.col("lon")) - F.radians(F.col("city_lon")))
    )
)

window_1 = Window.partitionBy('event.message_id').orderBy(F.asc('distance'))

enumeration_with_rank = enumeration_with_distance.withColumn('rank', F.row_number().over(window_1))

events_with_defined_city = enumeration_with_rank.filter(F.col('rank') == 1)
events_with_defined_city = events_with_defined_city.withColumn('event.datetime', F.to_timestamp(F.col('event.datetime')))

users_messages = events_with_defined_city.filter(F.col('event_type') == 'message' & F.col('event.message_from').isNotNull())

window_2 = Window.partitionBy('event.message_from').orderBy(F.desc('event.datetime'))

ranking_users_messages_by_date = users_messages.withColumn('rank', F.row_number().over(window_2))

latest_users_messages = ranking_users_messages_by_date.filter(F.col('rank') == 1)

latest_users_messages = latest_users_messages.withColumnRenamed('event.message_from', 'user_id')
latest_users_messages = latest_users_messages.withColumnRenamed('city', 'act_city')

data_needed_for_home_city = users_messages.select(F.col('event.message_from'), F.col('city'), F.col('event.datetime'))

data_needed_for_home_city = data_needed_for_home_city.withColumn('only_date', F.to_date(F.col('event.datetime')))

window_3 = Window.partitionBy('event.message_from', 'city').orderBy(F.asc('only_date'))

data_needed_for_home_city = data_needed_for_home_city.withColumn('days_counter', F.row_number().over(window_3))

data_needed_for_home_city = data_needed_for_home_city.withColumn ('group_id', F.col('only_date') - F.col('days_counter'))

data_needed_for_home_city = data_needed_for_home_city.groupBy('event.message_from', 'city', 'group_id').agg(F.count('*').alias('seq_length'))

data_needed_for_home_city = data_needed_for_home_city.withColumn('max_len', F.max('seq_length').over(Window.partitionBy('event.message_from')))

data_needed_for_home_city = data_needed_for_home_city.filter(F.col('seq_length') == F.col('max_len'))

data_needed_for_home_city = data_needed_for_home_city.filter(F.col('seq_length') >= 27)

home_city_research_result = data_needed_for_home_city.select(F.col('event.message_from'), F.col('city'))

home_city_research_result = home_city_research_result.withColumnRenamed('event.message_from', 'user_id')

data_needed_for_travel_count = users_messages.orderBy('event.message_from', 'event.datetime')

data_needed_for_travel_count = data_needed_for_travel_count.groupBy('event.message_from').agg(
    F.collect_list('city').alias('travel_array'),
    F.size(F.collect_list('city')).alias('travel_count')
)

travel_count_result = data_needed_for_travel_count.select('event.message_from', 'travel_array', 'travel_count')

travel_count_result = travel_count_result.withColumnRenamed('event.message_from', 'user_id')

ranking_users_events_by_date = events_with_defined_city.withColumn('rank', F.row_number().over(window_2))

latest_users_events = ranking_users_events_by_date.filter(F.col('rank') == 1)

latest_users_events = latest_users_events.withColumnRenamed('event.message_from', 'user_id')

timezone_data = [
    ('Sydney', 'Australia/Sydney'),
    ('Melbourne', 'Australia/Melbourne'),
    ('Brisbane', 'Australia/Brisbane'),
    ('Perth', 'Australia/Perth'),
    ('Adelaide', 'Australia/Adelaide'),
    ('Gold Coast', 'Australia/Gold Coast'),
    ('Cranbourne', 'Australia/Cranbourne'),
    ('Canberra', 'Australia/Canberra'),
    ('Newcastle', 'Australia/Newcastle'),
    ('Wollongong', 'Australia/Wollongong'),
    ('Geelong', 'Australia/Geelong'),
    ('Hobart', 'Australia/Hobart'),
    ('Townsville', 'Australia/Townsville'),
    ('Ipswich', 'Australia/Ipswich'),
    ('Cairns', 'Australia/Cairns'),
    ('Toowoomba', 'Australia/Toowoomba'),
    ('Darwin', 'Australia/Darwin'),
    ('Ballarat', 'Australia/Ballarat'),
    ('Bendigo', 'Australia/Bendigo'),
    ('Launceston', 'Australia/Launceston'),
    ('Mackay', 'Australia/Mackay'),
    ('Rockhampton', 'Australia/Rockhampton'),
    ('Maitland', 'Australia/Maitland'),
    ('Bunbury', 'Australia/Bunbury'),
]

timezone_df = spark.createDataFrame(timezone_data, ['city', 'timezone'])

latest_users_events = latest_users_events.join(timezone_df, on = ['city'], how = 'full outer')

latest_users_events = latest_users_events.withColumn(
    'local_time',
    F.from_utc_timestamp(F.col('event.datetime'), F.col('timezone'))
)

final_users_mart_project = latest_users_messages.join(home_city_research_result, on = ['user_id'], how = 'full outer')

final_users_mart_project = final_users_mart_project.join(travel_count_result, on = ['user_id'], how = 'full outer')

final_users_mart_project = final_users_mart_project.join(latest_users_events, on = ['user_id'], how = 'full outer')

final_users_mart = final_users_mart_project.select(
    F.col('user_id'),
    F.col('latest_users_messages.act_city'),
    F.col('home_city_research_result.city').alias('home_city'),
    F.col('travel_count_result.travel_array'),
    F.col('travel_count_result.travel_count'),
    F.col('latest_users_events.local_time')
)

final_users_mart.write \
    .format('parquet') \
    .mode('overwrite') \
    .path(my_analytics_layer_path)

window_4 = Window.partitionBy('event.message_from').orderBy(F.asc('event.datetime'))

ranking_users_messages_in_chronological_order = users_messages.withColumn('rank', F.row_number().over(window_4))

earliest_users_messages = ranking_users_messages_in_chronological_order.filter(F.col('rank') == 1)

geo_csv_my_copy = geo_csv_my_copy.withColumnRenamed('id', 'zone_id')

events_with_defined_city = events_with_defined_city.withColumn('month', F.month(F.col('event.datetime')))
events_with_defined_city = events_with_defined_city.withColumn('week', F.weekofyear(F.col('event.datetime')))

messages_prepared_for_the_final_mart = events_with_defined_city.filter(F.col('event_type') == 'message')
reactions_prepared_for_the_final_mart = events_with_defined_city.filter(F.col('event_type') == 'reaction')
subscriptions_prepared_for_the_final_mart = events_with_defined_city.filter(F.col('event_type') == 'subscription')
registrations_prepared_for_the_final_mart = earliest_users_messages.withColumn('week', F.weekofyear(F.col('event.datetime')))
registrations_prepared_for_the_final_mart = registrations_prepared_for_the_final_mart.withColumn('month', F.month(F.col('event.datetime')))

weekly_messages = messages_prepared_for_the_final_mart.groupBy('city', 'week').agg(F.count('*').alias('week_message'))
weekly_reactions = reactions_prepared_for_the_final_mart.groupBy('city', 'week').agg(F.count('*').alias('week_reaction'))
weekly_subscriptions = subscriptions_prepared_for_the_final_mart.groupBy('city', 'week').agg(F.count('*').alias('week_subscription'))
weekly_registrations = registrations_prepared_for_the_final_mart.groupBy('city', 'week').agg(F.count('*').alias('week_user'))

monthly_messages = messages_prepared_for_the_final_mart.groupBy('city', 'month').agg(F.count('*').alias('month_message'))
monthly_reactions = reactions_prepared_for_the_final_mart.groupBy('city', 'month').agg(F.count('*').alias('month_reaction'))
monthly_subscriptions = subscriptions_prepared_for_the_final_mart.groupBy('city', 'month').agg(F.count('*').alias('month_subscription'))
monthly_registrations = registrations_prepared_for_the_final_mart.groupBy('city', 'month').agg(F.count('*').alias('month_user'))

unique_data_and_cities = events_with_defined_city.select('city', 'week', 'month').distinct()

final_zones_mart_project = unique_data_and_cities \
    .join(geo_csv_my_copy, on=['city'], how='full outer') \
    .join(weekly_messages, on=['city', 'week'], how='full outer') \
    .join(weekly_reactions, on=['city', 'week'], how='full outer') \
    .join(weekly_subscriptions, on=['city', 'week'], how='full outer') \
    .join(weekly_registrations, on=['city', 'week'], how='full outer') \
    .join(monthly_messages, on=['city', 'month'], how='full outer') \
    .join(monthly_reactions, on=['city', 'month'], how='full outer') \
    .join(monthly_subscriptions, on=['city', 'month'], how='full outer') \
    .join(monthly_registrations, on=['city', 'month'], how='full outer')

final_zones_mart = final_zones_mart_project.select(
    F.col('month'),
    F.col('week'),
    F.col('zone_id'),
    F.col('week_message'),
    F.col('week_reaction'),
    F.col('week_subscription'),
    F.col('week_user'),
    F.col('month_message'),
    F.col('month_reaction'),
    F.col('month_subscription'),
    F.col('month_user'),
)