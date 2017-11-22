package ingestion.pipelines.services

import java.net.URLEncoder

import com.typesafe.config.ConfigFactory
import it.gov.daf.catalogmanager.client.Catalog_managerClient
import it.gov.daf.catalogmanager.{FlatSchema, MetaCatalog}
import play.api.libs.ws.WSClient

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object CatalogMgr {
  val uriCatalogManager = ConfigFactory.load().getString("WebServices.catalogManagerUrl")

  def apply(auth: String, ds_uri: String, loadingTimeout: Duration = 10 seconds)(implicit ws: WSClient, ec: ExecutionContext): CatalogMgr ={

    val logicalUriEncoded = URLEncoder.encode(ds_uri, "UTF-8")

    val catalogManagerClient = new Catalog_managerClient(ws)(uriCatalogManager)
    val response: Future[MetaCatalog] = catalogManagerClient.datasetcatalogbyid(auth,logicalUriEncoded)

    val metaCatalog: MetaCatalog = Await.result(response, Duration.Inf)
    new CatalogMgr(metaCatalog)
  }
}

class CatalogMgr(catalog: MetaCatalog) extends Serializable {


  val operational = catalog.operational
  val dataschema = catalog.dataschema

  val colName: Seq[String] = catalog.dataschema.flatSchema.map(field => field.name)


  val ontoTag: Map[String, String] = catalog.dataschema.flatSchema.map { field =>
    val metadata = field.metadata
    val semantics = metadata match {
      case Some(s) => s.semantics
      case None => None
    }
    field.name -> semantics
  }
    .filter(x=>(x._2.isDefined && !x._2.equals(""))).map(x=> x._1->x._2.get.id).toMap

}
