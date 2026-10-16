from pyspark import pipelines as dp


@dp.view
def dimuser_stg():
    """Stream the Silver DimUser table into the Gold CDC flow."""
    return spark.readStream.table("spotify_cata.silver.DimUser")


dp.create_streaming_table("dimuser")

# ADF incremental ingestion uses updated_at as the DimUser CDC watermark.
# Lakeflow uses the same source column to order changes for SCD Type 2 history.
dp.create_auto_cdc_flow(
    target="dimuser",
    source="dimuser_stg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type="2",
)
