
class reusable:

    def dropColumns(self, df, columns):

        df = df.drop(*columns)
        return df

    def deduplicateUserChanges(self, df):
        """Deduplicate a repeated CDC sequence, not every change to a user.

        Gold's DimUser SCD Type 2 flow sequences on updated_at. Streaming
        dropDuplicates(['user_id']) permanently suppresses subsequent
        updates to the same user across microbatches. Retaining updated_at
        in the deduplication key preserves distinct change versions.

        Note: streaming deduplication without a watermark retains state;
        a production deployment needs an explicit lateness/state policy.
        Upstream must ensure conflicting updates do not share a key/time.
        """
        return df.dropDuplicates(['user_id', 'updated_at'])
