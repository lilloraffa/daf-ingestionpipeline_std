import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import play.api.libs.ws.ahc.AhcWSClient
import play.api.libs.json._
import scala.concurrent.duration._

import scala.concurrent.{Await, ExecutionContext}

/**
  * Created by toddmcgrath on 6/15/16.
  */
object TestScalaSpark {

  def main(args: Array[String]) {
    val appName = "IngestionStd"
    val master = "local[*]"

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()

    //val df = spark.read.json("/Users/lilloraffa/voc_poi.json")

    implicit val ex = scala.concurrent.ExecutionContext.global

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val wsClient = AhcWSClient()
    //val jsonList = Json.parse(response.body).as[List[List[JsValue]]]
    saveVocPoi(wsClient, spark)

    saveDsPoi(spark)

    wsClient.close()

  }

  def saveVocPoi(wsClient: AhcWSClient, spark: SparkSession)(implicit ex: ExecutionContext) = {
    val res: String = Await.result(wsClient.url("http://localhost:9005/kb/v1/vocabularies/POICategoryClassification?lang=it").get().map(r => r.body), 10 seconds)
    println(res)
    val json = Json.parse(res).as[List[List[JsValue]]]

    val test = json.map{x1 =>
      x1.map{ x=>
        val k = (x \ "key").as[String]
        val v = (x\ "value").as[String]
        s""""$k":"$v""""
      }.mkString("{", ",", "}")
    }
    println(test)
    val rdd_test: RDD[String] = spark.sparkContext.parallelize(test)

    val df = spark.read.json(rdd_test)
    println(df.show())
    println(df.printSchema())

    df.write.mode("overwrite").parquet("data/voc_poi/")
  }

  def saveDsPoi(spark: SparkSession) = {
    val data = List(
      "{'name':'bar', 'comune': 'Santeramo', 'provincia': 'Bari', 'poiname': 'Settore intrattenimento'}",
      "{'name':'supermercato', 'comune': 'Andria', 'provincia': 'Bari', 'poiname': 'Settore shopping'}",
      "{'name':'ristorante', 'comune': 'Santeramo', 'provincia': 'Bari', 'poiname': 'Settore cibo'}",
      "{'name':'ristorante', 'comune': 'Santeramo', 'provincia': 'Bari', 'poiname': 'Settore cib'}",
      "{'name':'ristorante', 'comune': 'Santeramo', 'provincia': 'Bari', 'poiname': 'Settore cbo'}",
      "{'name':'bettola', 'comune': 'Gioia', 'provincia': 'Bari', 'poiname': 'Settore cibo'}",
      "{'name':'supermercato', 'comune': 'Roma', 'provincia': 'Roma', 'poiname': 'Settore shopping/acq'}"
    )

    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.read.json(rdd)
    df.write.mode("overwrite").parquet("data/ds_poi")
  }

}
