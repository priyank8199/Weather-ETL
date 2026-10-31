CATALOG = "workspace"
SCHEMA = "weather"
SOURCE = f"{CATALOG}.{SCHEMA}.weather_canada"

df = spark.table(SOURCE)
print(f"Rows: {df.count()}, cities: {df.select('city', 'province').distinct().count()}")



from pyspark.sql import functions as F

wmo_codes = [
    (0, "Clear sky"), (1, "Mainly clear"), (2, "Partly cloudy"), (3, "Overcast"),
    (45, "Fog"), (48, "Rime fog"),
    (51, "Light drizzle"), (53, "Drizzle"), (55, "Dense drizzle"),
    (56, "Freezing drizzle"), (57, "Dense freezing drizzle"),
    (61, "Light rain"), (63, "Rain"), (65, "Heavy rain"),
    (66, "Freezing rain"), (67, "Heavy freezing rain"),
    (71, "Light snow"), (73, "Snow"), (75, "Heavy snow"), (77, "Snow grains"),
    (80, "Light showers"), (81, "Showers"), (82, "Violent showers"),
    (85, "Snow showers"), (86, "Heavy snow showers"),
    (95, "Thunderstorm"), (96, "Thunderstorm with hail"), (99, "Severe thunderstorm with hail"),
]
codes_df = spark.createDataFrame(wmo_codes, ["weathercode", "condition"])

weather = df.join(codes_df, on="weathercode", how="left")


from pyspark.sql.window import Window

latest_window = Window.partitionBy("city", "province").orderBy(F.col("observed_at").desc())

latest = (
    weather
    .withColumn("rn", F.row_number().over(latest_window))
    .filter("rn = 1")
    .drop("rn")
    .select("city", "province", "temperature", "windspeed", "condition", "observed_at")
    .orderBy(F.col("temperature").desc())
)
display(latest)


province_summary = (
    weather
    .groupBy("province")
    .agg(
        F.round(F.avg("temperature"), 1).alias("avg_temp_c"),
        F.round(F.min("temperature"), 1).alias("min_temp_c"),
        F.round(F.max("temperature"), 1).alias("max_temp_c"),
        F.round(F.avg("windspeed"), 1).alias("avg_wind_kmh"),
        F.count("*").alias("readings"),
    )
    .orderBy(F.col("avg_temp_c").desc())
)
display(province_summary)



daily_trend = (
    weather
    .withColumn("date", F.to_date("observed_at"))
    .groupBy("date", "city", "province")
    .agg(
        F.round(F.avg("temperature"), 1).alias("avg_temp_c"),
        F.round(F.avg("windspeed"), 1).alias("avg_wind_kmh"),
    )
    .orderBy("date", "city")
)
display(daily_trend)



day_window_desc = Window.partitionBy("date").orderBy(F.col("avg_temp_c").desc())
day_window_asc = Window.partitionBy("date").orderBy(F.col("avg_temp_c").asc())

extremes = (
    daily_trend
    .withColumn("warm_rank", F.row_number().over(day_window_desc))
    .withColumn("cold_rank", F.row_number().over(day_window_asc))
    .filter("warm_rank = 1 OR cold_rank = 1")
    .withColumn("type", F.when(F.col("warm_rank") == 1, "Warmest").otherwise("Coldest"))
    .select("date", "type", "city", "province", "avg_temp_c")
    .orderBy("date", "type")
)
display(extremes)



condition_counts = (
    weather
    .groupBy(F.coalesce("condition", F.lit("Unknown")).alias("condition"))
    .count()
    .orderBy(F.col("count").desc())
)
display(condition_counts)



outputs = {
    "latest_by_city": latest,
    "province_summary": province_summary,
    "daily_trend": daily_trend,
    "daily_extremes": extremes,
    "condition_counts": condition_counts,
}

for name, frame in outputs.items():
    frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{CATALOG}.{SCHEMA}.{name}"
    )
    print(f"Saved {CATALOG}.{SCHEMA}.{name}")
