package ingestion.pipelines

import ingestion.pipelines.data.IngestionClass.IngPipeReport
import ingestion.pipelines.services.CatalogMgr
import org.apache.spark.sql.DataFrame

trait IngestionPipeline {
  def initBatch(ds_uri: String, dataCatMgr: CatalogMgr, df_data: DataFrame): (Option[DataFrame], IngPipeReport)
  def initStream(ds_uri: String, dataCatMgr: CatalogMgr, df_data: DataFrame): (Option[DataFrame], IngPipeReport)
}
