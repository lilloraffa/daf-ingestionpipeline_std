package ingestion

import ingestion.pipelines.data.IngestionClass.{IngPipeReport, IngestionReport}
import ingestion.pipelines.services.{CatalogMgr, DatasetLoader}
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.ws.WSClient

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext

class IngestionPipelineProc(ds_uri: String, ingPipelines: List[String])(implicit ws: WSClient, ex: ExecutionContext, spark: SparkSession, auth: String) {

  val dataCatMgr: CatalogMgr = CatalogMgr(auth = auth, ds_uri = ds_uri)

  def start(): IngestionReport = {
    val dfStartOpt: Option[DataFrame] = DatasetLoader.dataset_loadDF(ds_uri, spark)
    start(dfStartOpt)
  }

  def start(dfStartOpt: Option[DataFrame]): IngestionReport = {

    dfStartOpt match {
      case Some(dfStart) =>
        val (df, ingPipeReports) = doPipeline(dfStart, ingPipelines)
        println("doPipeline finished")
        dataCatMgr.operational.dataset_type match {
          case "batch" => saveBatch(Some(df), ingPipeReports)
          case "stream" => saveStream(Some(df), ingPipeReports)
        }
      case None => saveBatch(None, List())
    }

  }

  //@tailrec
  def doPipeline(df: DataFrame, ingPipelines: List[String]): (DataFrame, List[IngPipeReport]) ={
    def doPipelineAcc(df: DataFrame, ingPipelines: List[String], ingPipeReports: List[IngPipeReport]): (DataFrame, List[IngPipeReport]) = {

      ingPipelines match {
        case Nil => (df, ingPipeReports)
        case x :: tail => {
          val ingPipeline = IngestionPipelineMapper.get(x)
          val (dfOpt, ingPipeReport) = dataCatMgr.operational.dataset_type match {
            case "batch" => ingPipeline.initBatch(ds_uri, dataCatMgr, df)
            case "stream" => ingPipeline.initStream(ds_uri, dataCatMgr, df) //TODO Rework when finalize stream stuff. It may not be necessary if we generalize batch, but need to consider performance issues
          }

          dfOpt match {
            case Some(dfNew) => doPipelineAcc(dfNew, tail, ingPipeReports ::: List(ingPipeReport))
            case _ => doPipelineAcc(df, tail, ingPipeReports ::: List(ingPipeReport)) //In case the pipeline fail, it continues with the next one and keep track in the ingReport
          }

        }
      }

    }

    doPipelineAcc(df, ingPipelines, List())
  }

  def saveBatch(dfOpt: Option[DataFrame], ingReports: List[IngPipeReport]): IngestionReport = {
    val pathBase = "data"
    val pathDs = s"${dataCatMgr.operational.group_own}/${dataCatMgr.operational.theme}/${dataCatMgr.operational.subtheme}/${dataCatMgr.dataschema.avro.name}"
    val path = s"$pathBase/ingPipelines"
    dfOpt match {
      case Some(df) =>
        try {
          df.show()
          df.write.parquet(path)
          IngestionReport(ds_uri, "dateStart", "dateEnd", path, ingReports, "OK")
        } catch {
          case _ => IngestionReport(ds_uri, "dateStart", "dateEnd", path, ingReports, "ERROR on Saving")
        }
      case None => IngestionReport(ds_uri, "dateStart", "dateEnd", path, ingReports, "ERROR")
    }

  }

  def saveStream(dfOpt: Option[DataFrame], ingReports: List[IngPipeReport]): IngestionReport = {
    saveBatch(dfOpt, ingReports)
  }
}
