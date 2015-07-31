package data
import spray.json.DefaultJsonProtocol._
case class Tweets(timeInterval : String, tweetsCount:Long)

object Tweets {
  implicit val tweetsJson = jsonFormat2(apply)

}