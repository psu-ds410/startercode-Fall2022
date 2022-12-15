import org.apache.spark.SparkConf

object fbcountrdd{
  def main(args: Array[String]) {
    val conf = new
SparkConf().setAppname("fbcountrdd")
    val sc = new SparkContext(conf)

    val input =
sc.textFile("/datasets/facebook")
    val pairs = input.map(line =>
line.split(" ")).filter(pair =>
pair(1).toInt > 500)
    val counts = pairs.map(pair =>
(pair(0), 1)).reduceByKey((a, b) =>
a + b)
    val output = counts.filter(pair
=> pair._2 > 2).map(pair =>
(pair._1, pair._2 + 1))

output.saveAsTextFile("fbcountrdd")
    }
}
