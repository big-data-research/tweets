package data

import spray.json.DefaultJsonProtocol._
case class Tweeps(timeInterval : String, tweeps : Array[(String, String)])

object Tweeps {
  implicit val tweepsJson = jsonFormat2(apply)

}