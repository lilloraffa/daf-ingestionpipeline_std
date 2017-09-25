import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object ScalaSpark {
  def main(args: Array[String]) {

    val logFile = "/Users/lilloraffa/Development/teamdgt/daf/daf/README.md" // Should be some file on your system
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkTest")
      .getOrCreate()
    println("ciao ciao")
    val logData = spark.read.textFile(logFile).cache
    print(logData.getClass)
    //val numAs = logData.filter(line => line.contains("a")).count()
    //val numBs = logData.filter(line => line.contains("b")).count()
    //println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}

