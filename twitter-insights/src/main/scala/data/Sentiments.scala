package data
import spray.json.DefaultJsonProtocol._

case class Sentiments(timeInterval : String, sentiments : Array[(String, String)])

object Sentiments {
  implicit val sentimentJson = jsonFormat2(apply)

}