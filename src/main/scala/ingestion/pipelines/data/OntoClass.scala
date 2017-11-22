package ingestion.pipelines.data

import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Reads}

object OntoClass {
  case class OntoData(vocName: String, vocCol: String, hierc: List[String] = List.empty)
  case class Annotation(
                         vocabulary_id: String,
                         vocabulary: String,
                         ontology: String,
                         semantic_annotation: String,
                         property_id: String,
                         concept_id: String,
                         ontology_prefix: String,
                         ontology_id: String,
                         concept: String,
                         property: String
                       )

  implicit val annotationReads: Reads[Annotation] = (
    (JsPath \ "vocabulary_id").read[String] and
      (JsPath \ "vocabulary").read[String] and
      (JsPath \ "ontology").read[String] and
      (JsPath \ "semantic_annotation").read[String] and
      (JsPath \ "property_id").read[String] and
      (JsPath \ "concept_id").read[String] and
      (JsPath \ "ontology_prefix").read[String] and
      (JsPath \ "ontology_id").read[String] and
      (JsPath \ "concept").read[String] and
      (JsPath \ "property").read[String]
  )(Annotation.apply _)


}
