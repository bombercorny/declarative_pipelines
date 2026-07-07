from declarative_pipelines.connection import spark

df1 = spark.createDataFrame([(1, 2), (3, 4), (9, 9)], ["col1", "col2"])

df2 = spark.createDataFrame([(5, 6), (7, 8), (10, 9)], ["col1", "col2"])

df1.write.mode("overwrite").saveAsTable("catalog.schema.sdp_source1")
df2.write.mode("overwrite").saveAsTable("catalog.schema.sdp_source2")
