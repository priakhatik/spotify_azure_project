# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys

project_pth = os.path.abspath(os.path.join(os.getcwd(), '..', '..', '..'))
sys.path.append(project_pth)

from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ####DIMUSER

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####AUTOLOADER

# COMMAND ----------

# DBTITLE 1,ransf
df_user = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@storagespotifyproject.dfs.core.windows.net/DimUser/checkpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@storagespotifyproject.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df_user, checkpointLocation="abfss://silver@storagespotifyproject.dfs.core.windows.net/DimUser/checkpoint/cp_display")

# COMMAND ----------

df_user = df_user.withColumn("user_name", upper(col("user_name")))
display(df_user, checkpointLocation="abfss://silver@storagespotifyproject.dfs.core.windows.net/DimUser/checkpoint/cp_display_upper")

# COMMAND ----------

df_user_obj = reusable()

df_user = df_user_obj.dropColumns(df_user,['_rescued_data'])
df_user = df_user.dropDuplicates(['user_id'])
display(df_user ,checkpointLocation="abfss://silver@storagespotifyproject.dfs.core.windows.net/DimUser/checkpoint/cp_drop")

# COMMAND ----------

df_user.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","abfss://silver@storagespotifyproject.dfs.core.windows.net/DimUser/checkpoint/final")\
            .trigger(once=True)\
            .option("path","abfss://silver@storagespotifyproject.dfs.core.windows.net/DimUser/data")\
            .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ####DIMARTIST

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@storagespotifyproject.dfs.core.windows.net/DimArt/checkpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@storagespotifyproject.dfs.core.windows.net/DimArtist")

# COMMAND ----------

display(df_art, checkpointLocation="abfss://silver@storagespotifyproject.dfs.core.windows.net/DimArt/checkpoint/cp_display")

# COMMAND ----------

df_art_obj = reusable()

df_art = df_art_obj.dropColumns(df_art,['_rescued_data'])
df_art = df_art.dropDuplicates(['artist_id'])
display(df_art, checkpointLocation="abfss://silver@storagespotifyproject.dfs.core.windows.net/DimArt/checkpoint/cp_drop")

# COMMAND ----------

df_art.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","abfss://silver@storagespotifyproject.dfs.core.windows.net/DimArt/checkpoint/final")\
            .trigger(once=True)\
            .option("path","abfss://silver@storagespotifyproject.dfs.core.windows.net/DimArt/data")\
            .toTable("spotify_cata.silver.DimArt")

# COMMAND ----------

# MAGIC %md
# MAGIC ####DIMTRACK

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@storagespotifyproject.dfs.core.windows.net/DimTrack/checkpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@storagespotifyproject.dfs.core.windows.net/DimTrack")

# COMMAND ----------

display(df_track, checkpointLocation="abfss://silver@storagespotifyproject.dfs.core.windows.net/DimTrack/checkpoint/cp_display")

# COMMAND ----------

df_track = df_track.withColumn("durationFlag",when(col('duration_sec')<150,"low")\
                                            .when(col('duration_sec')<300,"medium")\
                                            .otherwise("high"))

df_track = df_track.withColumn("track_name",regexp_replace(col('track_name'),'-',' '))

df_track = reusable().dropColumns(df_track,['_rescued_data'])

display(df_track, checkpointLocation="abfss://silver@storagespotifyproject.dfs.core.windows.net/DimTrack/checkpoint/cp_modified")

# COMMAND ----------

df_track.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","abfss://silver@storagespotifyproject.dfs.core.windows.net/DimTrack/checkpoint/final")\
            .trigger(once=True)\
            .option("path","abfss://silver@storagespotifyproject.dfs.core.windows.net/DimTrack/data")\
            .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ####DIMDATE

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@storagespotifyproject.dfs.core.windows.net/DimDate/checkpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@storagespotifyproject.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date = reusable().dropColumns(df_date,['_rescued_data'])

df_date.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","abfss://silver@storagespotifyproject.dfs.core.windows.net/DimDate/checkpoint/final")\
            .trigger(once=True)\
            .option("path","abfss://silver@storagespotifyproject.dfs.core.windows.net/DimDate/data")\
            .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ####FACTSTREAM

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","abfss://silver@storagespotifyproject.dfs.core.windows.net/FactStream/checkpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
            .load("abfss://bronze@storagespotifyproject.dfs.core.windows.net/FactStream")

# COMMAND ----------

display(df_fact, checkpointLocation="abfss://silver@storagespotifyproject.dfs.core.windows.net/FactStream/checkpoint/cp_display")

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact,['_rescued_data'])

df_fact.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation","abfss://silver@storagespotifyproject.dfs.core.windows.net/FactStream/checkpoint/final")\
            .trigger(once=True)\
            .option("path","abfss://silver@storagespotifyproject.dfs.core.windows.net/FactStream/data")\
            .toTable("spotify_cata.silver.FactStream")