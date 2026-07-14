import dlt

@dlt.table 
def dimuser_stg():
    df = spark.readStream.table("spotify_cata.silver.DimUser")
    return df


dlt.create_streaming_table("dimuser")

dp.create_auto_cdc_flow(
  target = "dimuser",
  source = "dimuser_stg",
  keys = ["user_id"],
  sequence_by = "<updated_at>",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  name = None,
  once = False
)