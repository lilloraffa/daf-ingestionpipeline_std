package ingestion

import ingestion.pipelines.{IngestionPipeline, IngestionStd}
import org.apache.spark.sql.SparkSession
import play.api.libs.ws.WSClient

import scala.concurrent.ExecutionContext

object IngestionPipelineMapper {
  def get(ingPipelineName: String)(implicit ws: WSClient, ex: ExecutionContext, spark: SparkSession, auth: String): IngestionPipeline = {
    ingPipelineName match {
      case "IngestionStd" =>  new IngestionStd
    }
  }
}
