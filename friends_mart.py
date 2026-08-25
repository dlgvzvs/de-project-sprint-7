from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
from datetime import datetime

master_data_base_path = '/user/master/data/'
my_data_base_path = '/user/s25321830/data/'
my_raw_layer_path = f'{my_data_base_path}/raw'
my_analytics_layer_path = f'{my_data_base_path}/analytics'
geo_csv_path = '/user/s25321830/geo/geo.csv'

spark = SparkSession.builder \
                    .appName('app_1_temp_name') \
                    .master('yarn') \
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

subscribers_of_every_channel = events_with_defined_city.filter(F.col('event.subscription_user').isNotNull() & \
                                                               F.col('event.subscription_channel').isNotNull() & \
                                                               F.col('event_type') == 'subscription')

only_subscribers_and_channels = subscribers_of_every_channel.select(F.col('event.subscription_user').alias('subscription_user'), F.col('event.subscription_channel').alias('subscription_channel')).distinct()

only_subscribers_and_channels = only_subscribers_and_channels groupBy('subscription_channel').agg(
    F.collect_list('subscription_user').alias('list_of_subscribers')
)

exploded_subscribers = only_subscribers_and_channels.select(
    F.col('subscription_channel'),
    F.explode(F.col('list_of_subscribers')).alias('subscription_user')
)

left = exploded_subscribers.alias('left')
right = exploded_subscribers.alias('right')

pairs = left.join(right, on=['subscription_channel'], how='inner')

pairs = pairs.filter(F.col('left.subscription_user') < F.col('right.subscription_user'))

unique_pairs = pairs.select(
    F.col('left.subscription_user').alias('user_left'),
    F.col('right.subscription_user').alias('user_right')
).distinct()

common_channels = pairs.groupBy(
    F.col('left.subscription_user').alias('user_left'),
    F.col('right.subscription_user').alias('user_right')
).agg(
    F.count('*').alias('common_channels')
)

people_with_chat_history = events_with_defined_city.filter(F.col('event_type') == 'message' & F.col('event.message_from').isNotNull() & F.col('event.message_to').isNotNull())

people_with_chat_history = people_with_chat_history.select(
    F.least('user_left', 'user_right').alias('user_left'),
    F.greatest('user_left', 'user_right').alias('user_right')
).distinct()

people_without_chat_history = common_channels.join(
    people_with_chat_history,
    on=['user_left', 'user_right'],
    how='left_anti'
)

final_users_mart = spark.read.parquet(f'{my_analytics_layer_path}/users_mart')

left_half = final_users_mart.select(
    F.col('user_id').alias('user_left'),
    F.col('act_city').alias('city_left')
    )

right_half = final_users_mart.select(
    F.col('user_id').alias('user_right'),
    F.col('act_city').alias('city_right')
    )

candidates = people_without_chat_history \
    .join(left_half, on=['user_left'], how='inner') \
    .join(right_half, on=['user_right'], how='inner')

filtered_by_city = candidates.filter(F.col('city_left') == F.col('city_right'))

users_with_zones = final_users_mart \
    .join(geo_csv_my_copy, on=['act_city'], how='left') \
    .select('user_id', 'act_city', 'zone_id')

distance_between_users = F.lit(6371) * F.acos(
    F.sin(F.radians(F.col("lat_left"))) * F.sin(F.radians(F.col("lat_right"))) +
    F.cos(F.radians(F.col("lat_left"))) * F.cos(F.radians(F.col("lat_right"))) *
    F.cos(F.radians(F.col("lon_left")) - F.radians(F.col("lon_right")))
).alias('distance')

final_friends_recommendations_mart_project = filtered_by_city \
    .join(users_with_zones.select('user_id', 'zone_id').alias('left_zone'), 
          on=[F.col('left_zone.user_id') == F.col('filtered_by_city.user_left')], 
          how='left') \
    .join(users_with_zones.select('user_id', 'zone_id').alias('right_zone'), 
          on=[F.col('right_zone.user_id') == F.col('filtered_by_city.user_right')], 
          how='left')

processed_dttm = datetime.now()

data_for_latest_users_location = events_with_defined_city.select(
    F.col('event.city_lat').alias('city_lat'),
    F.col('event.city_lon').alias('city_lon'),
    F.col('event.message_from').alias('user_id'),
    F.col('event.datetime').alias('datetime')
)

window_for_latest_location = Window.partitionBy('user_id').orderBy(F.desc('datetime'))
ranking_users_events_by_date_yet_again = data_for_latest_users_location.withColumn('rank', F.row_number().over(window_for_latest_location))
latest_users_location = ranking_users_events_by_date_yet_again.filter(F.col('rank') == 1)

step1 = filtered_by_city.join(
    latest_users_location,
    filtered_by_city.user_left == latest_users_location.user_id,
    how='left'
)

step2 = step1.join(
    latest_users_location,
    step1.user_right == latest_users_location.user_id,
    how='left'
)

latest_users_location = events_with_defined_city \
    .select(
        F.col('event.message_from').alias('user_id'),
        F.col('city_lat'),
        F.col('city_lon'),
        F.col('event.datetime').alias('datetime')
    ) \
    .withColumn(
        'rank', 
        F.row_number().over(Window.partitionBy('user_id').orderBy(F.desc('datetime')))
    ) \
    .filter(F.col('rank') == 1) \
    .select('user_id', 'city_lat', 'city_lon')

with_left = filtered_by_city.join(
    latest_users_location.select(
        F.col('user_id').alias('left_id'),
        F.col('city_lat').alias('left_lat'),
        F.col('city_lon').alias('left_lon')
    ),
    on=[F.col('left_id') == F.col('user_left')],
    how='left'
)

with_both = with_left.join(
    latest_users_location.select(
        F.col('user_id').alias('right_id'),
        F.col('city_lat').alias('right_lat'),
        F.col('city_lon').alias('right_lon')
    ),
    on=[F.col('right_id') == F.col('user_right')],
    how='left'
)

with_zones = with_both.join(
    final_users_mart.select('user_id', 'zone_id', 'local_time').alias('user_info'),
    on=[F.col('user_info.user_id') == F.col('user_left')],
    how='left'
)

with_distance = with_zones.withColumn(
    'distance',
    F.lit(6371) * F.acos(
        F.sin(F.radians(F.col("left_lat"))) * F.sin(F.radians(F.col("right_lat"))) +
        F.cos(F.radians(F.col("left_lat"))) * F.cos(F.radians(F.col("right_lat"))) *
        F.cos(F.radians(F.col("left_lon")) - F.radians(F.col("right_lon")))
    )
)

final_friends_recommendations_mart = with_distance \
    .filter(F.col('distance') <= 1) \
    .select(
        F.col('user_left'),
        F.col('user_right'),
        F.lit(datetime.now()).alias('processed_dttm'),
        F.col('zone_id'),
        F.col('local_time')
    )

final_friends_recommendations_mart.write \
    .format('parquet') \
    .mode('overwrite') \
    .path(f'{my_analytics_layer_path}/friends_recommendations_mart')