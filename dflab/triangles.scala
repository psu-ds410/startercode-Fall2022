//import statements
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import session.implicits._

val session = SparkSession.builder().getOrCreate()

object DFlab {
    
    def getFB(sc: SparkContext): df.map[Array[String]] = {
       sc.textFile("/datasets/facebook").map{x => x.trim()}.map{x => x.split(" ")}
    }

    def getFBTuples(sc: SparkContext): df.map[(String, String)] = {
        getFB(sc).map{x => (x(0), x(1))}
        
   }
    //val dfFromRDD1 = rdd.toDF()
    //dfFromRDD1.printSchema()
   }
    def getTestDataFrame(spark) = {
      List((1,2,3), (4,5,6)).toDF("a", "b", "c")
   }
    
    val data_location = ("/datasets/facebook")
    val df = spark.read.format("csv").load(data_location)
    df.printSchema

   }
    import org.apache.spark.sql.types._
    val mySchema = new StructType()
               .add("Triangles", LongType, true)
               //.add("Count", LongType, false)
    val df2 = spark.read.format("json").schema(mySchema).load("data_location")
    df2.show()

   }
    def numTriangles(graph: df.map[(String, String)]) = {
        val flipped = graph.map{case (a, b) => (b, a)}
        val combined = graph.union(flipped).distinct()
        val selfjoin = combined.join(combined)       
        val cleaned = selfjoin.filter{case (mid, (start, end)) => start != end}
        val flipped_clean = cleaned.map{case (a, b) => (b,a)}
        val hacked_combined = combined.map{x => (x, 1)}
        val all_joined = flipped_clean.join(hacked_combined)
        all_joined.count() / 6
    }  
    
    //saving file 
    df.write.format("parquet").save("hdfs://datasets/facebook/FacebookTriangles")
    }
}
