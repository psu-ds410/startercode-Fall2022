import org.apache.spark.sql.functions._

val df = spark.read
   .option("delimiter", " ")
   .csv("/datasets/facebook")

val fbcountdf =
df.withColumn("left",
split(col("_c0"), " ")(0))
   .withColumn("right",
split(col("_c0"), " "(1))
   .filter(col("right").cast("int") > 500)
   .groupBy("left")
   .count()
   .filter(col("count") > 2)
   .withColumn("count", col("count") + 1)

fbcountdf.write.save("fbcount_df")
