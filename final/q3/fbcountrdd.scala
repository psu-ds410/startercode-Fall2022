import org.apache.spark.rdd.RDD

val rdd: RDD[String] = spark.sparkContext.textFile("/datasets/facebook")

val fbcountrdd = rdd
  .map(_.split(" "))
  .filter(line => line(1).toInt > 500)
  .map(line => (line(0), 1))
  .reduceByKey(_ + _)
  .filter(_._2 > 2)
  .mapValues(_ +1)

fbcountrdd.saveAsTextFile("fbcount_rdd")
