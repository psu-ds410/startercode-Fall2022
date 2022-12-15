import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Q2 extends App {
  def main(args: Array[String]) = {
    def getSC() = {
      val conf = new SparkConf().setAPPName("q3")
      val sc = new SparkContent(conf)
      sc
    }
    def getRDD(sc: SparkContext) = {
      val input = sc.textfile("/datasets/flight")
      input
    }
    
    def hubAirport(input: RDD[String]) = {
      val flights = sc.textfile("/datasets/flight")
      val noheader = flights.filter{line => !line.startsWith("ITIN_ID")}
      val records = noheader.map{x => x.split(",")}
      val outgoing = records.map{record => (records(3), -record(7).toFloat)}
      val incoming = records.map{record => (records(5), record(7).toFloat)}
      val counts = incoming - outgoing

      def zeroVal = (0.0,0.0)
      def func = (state: (Float, Float), v: (Float, Float)) = state._1 + v._1, state._2 + v._2)
      val result = both.aggregateByKey(zeroVal)(func, func)
      val hub = result.filter{case (airport, (incoming, outgoing)) => incoming >= y}
      hub
    }
    def saveit(counts: org.apache.spark.rdd.RDD[(String, Int)} = {
      counts.saveAsTextFile(result, "sparkhw-q3")

      
    // call your code using spark-submit nameofjarfile.jar commandlinearg
    val cmd_arg = args(0) // this is the commandlinearg
  }
