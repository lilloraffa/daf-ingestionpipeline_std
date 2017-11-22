package ingestion.pipelines

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import ingestion.IngestionPipelineProc
import org.apache.spark.sql.SparkSession
import play.api.libs.ws.ahc.AhcWSClient

object IngestionProc {
  def main(args: Array[String]): Unit = {
    val appName = "IngestionStd"
    val master = "local[*]"

    implicit val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()


    implicit val ex = scala.concurrent.ExecutionContext.global
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val wsClient = AhcWSClient()
    implicit val auth = "Basic YWxlOmFsZQ=="

    //val ing = new IngestionStd()
    //ing.initBatch("daf://dataset/ord/raffaele/raffaele/theme/ds_poi")
    val ds_uri = "daf://dataset/ord/raffaele/raffaele/theme/ds_poi"
    val ingPipelines = List("IngestionStd")

    val ingProc = new IngestionPipelineProc(ds_uri, ingPipelines)
    ingProc.start()
    spark.stop()

  }
}
