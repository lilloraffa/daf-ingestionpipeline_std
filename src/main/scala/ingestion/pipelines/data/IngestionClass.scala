package ingestion.pipelines.data

object IngestionClass {
  case class IngPipeReport(dsUri: String,
                           startTime: String,
                           endTime: String,
                           pipelineName: String,
                           body: String)

  case class IngestionReport(dsUri: String,
                             startTime: String,
                             endTime: String,
                             path: String,
                             pipelines: List[IngPipeReport],
                             status: String)
}
