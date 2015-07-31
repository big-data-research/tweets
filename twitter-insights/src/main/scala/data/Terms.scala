package data

import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Terms(timeInterval : String, terms : Array[(String, String)])

object Terms {
  implicit val termsJson = jsonFormat2(apply)

}