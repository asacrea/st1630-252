"""Small, reusable Delta Lake write primitives."""
from delta.tables import DeltaTable


def merge_insert(spark, frame, path, condition):
    """Insert rows not already represented by ``condition``."""
    if frame.isEmpty():
        return
    if DeltaTable.isDeltaTable(spark, path):
        (DeltaTable.forPath(spark, path).alias("target")
         .merge(frame.alias("source"), condition)
         .whenNotMatchedInsertAll()
         .execute())
    else:
        frame.write.format("delta").mode("errorifexists").save(path)


def merge_upsert(spark, frame, path, condition):
    """Idempotently update existing business keys and insert new ones."""
    if frame.isEmpty():
        return
    if DeltaTable.isDeltaTable(spark, path):
        (DeltaTable.forPath(spark, path).alias("target")
         .merge(frame.alias("source"), condition)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    else:
        frame.write.format("delta").mode("errorifexists").save(path)


def replace_table(frame, path):
    """Atomically replace a fully recomputed aggregate using a Delta transaction."""
    (frame.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").save(path))
