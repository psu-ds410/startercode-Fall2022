//import statements
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import session.implicits._

val session = SparkSession.builder().getOrCreate()

object DFlab {
    
    val triangles = numTriangles(test_df)
    saveIt(triangles, "FacebookTriangles")
   }
    def getTestDataFrame(spark) = {
      List((1,2,3), (4,5,6)).toDF("a", "b", "c")
   }
    
    def getSC() = {
        val conf = new SparkConf().setAppName("q1")
        val sc = new SparkContext(conf)
        sc

   }
    import org.apache.spark.sql.types._
    val mySchema = new StructType()
               .add("Triangles", LongType, true)
               //.add("Count", LongType, false)
    val df2 = spark.read.format("json").schema(mySchema).load("data_location")
    df2.show()

   }
    def numTriangles(sc: SparkContext, df: DataFrame) = {
        val df_test = df
        val df_facebook = getDataFrame()
        
        val flipped = df_facebook.select("Col2", "Col1")
        val combined = df_facebook.union(flipped).distinct()
        
        val df_facebook2 = df_facebook.withColumnRenamed("Col1"), "newCol1")
        val selfjoin = df_facebook.join(df_facebook2, df_facebook.col("Col1") == df_facebook2.col("newCol1"))
        
        val cleaned = selfjoin.where((col("Col2".getItem(0) =!= col("Col2".getItem(1))
                                         
        val flipped_clean = cleaned.select("Col2", "Col1")
        val hacked_combined = combined.select("Col2").distinct().count()
        val all_joined = flipped_clean.join(hacked_combined)
        val all_joined.select(count("*")) / 6                                  
    }  
    
}
