package ingestion.pipelines.services

import com.typesafe.config.ConfigFactory
import ingestion.pipelines.data.OntoClass.{Annotation, OntoData}
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{JsArray, JsValue, Json}
import play.api.libs.ws.WSClient

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

object OntoMgr {
  /**
    *
    * @param semanticTags
    * @param loadingTimeout
    * @param ws
    * @param ec
    * @return
    */
  def apply(semanticTags: List[String], loadingTimeout: Duration = 10 seconds)(implicit ws: WSClient, ec: ExecutionContext): OntoMgr = {

    val semanticAnnotationUrl: String = ConfigFactory.load().getString("SemanticSrv.Annotation")
    val semanticHiercUrl: String = ConfigFactory.load().getString("SemanticSrv.Hierc")


    def getVocabulary(tag: String): Future[OntoData] = {

      //ws.url(s"http://localhost:9005/kb/v1/daf/annotation/lookup?semantic_annotation=$tag").get()
      ws.url(s"$semanticAnnotationUrl?semantic_annotation=$tag").get()
        .map{ response =>

          val jsonArr = Json.parse(response.body).as[JsArray]
          val annotation = jsonArr(0).get.as[Annotation]

          val idTripList = List(annotation.ontology_id,
            annotation.concept_id, annotation.property_id)

          extractHierachy(annotation.vocabulary_id, annotation.ontology_prefix, idTripList)
            .map(paths => OntoData(annotation.vocabulary, idTripList.mkString("_"), paths))
        }.flatMap(identity)
    }

    def extractHierachy(vocId: String, ontoName: String, idTripName: List[String]): Future[List[String]] = {
      ws.url(s"$semanticHiercUrl?vocabulary_name=$vocId&ontology_name=$ontoName&lang=it").get()
        .map { response =>
          val jsonList = Json.parse(response.body).as[List[JsValue]]
          println(idTripName.mkString("."))
          jsonList.filter(x => (x \ "path").get.as[String].toLowerCase.equals(idTripName.mkString(".").toLowerCase))
            .map{ x =>
              (x \ "path").get.as[String]
            }
        }
    }

    val ontoMap: Map[String, OntoData] = semanticTags.map{ tag =>
      (tag -> Await.result(getVocabulary(tag), loadingTimeout))
    } toMap

    new OntoMgr(ontoMap)
  }

}
class OntoMgr(ontoData: Map[String, OntoData]) {
  def getDicDF(dicName: String, spark: SparkSession): Option[DataFrame] = DatasetLoader.dataset_loadDF(dicName, spark)
  def getOntoInfoField(tagName: String): OntoData = ontoData(tagName)
  def getOnto(): Map[String, OntoData] = ontoData
}
