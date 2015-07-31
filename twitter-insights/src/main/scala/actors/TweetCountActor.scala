package actors

import akka.actor.Actor
import scala.io.Source
import bigdataresearch.Configuration
import scala.util.Try
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils
import org.elasticsearch.spark._
import scala.concurrent._
import ExecutionContext.Implicits.global
import messages.ProcessTwitterStream
import com.google.gson.GsonBuilder
import org.apache.spark.SparkContext
import messages.CountTweets
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import customs.Utils
import scalaj.http.Http
import data.Tweets

class TweetCountActor extends Actor {

  override def receive = {

    case message: CountTweets => {

      Configuration.log4j.info(s"[TweetsCountActor]: Counting tweets for serch term ${message.searchTerm} for the last ${message.mins} mins")
      val currentSender = sender

      val urlStr = s"${Configuration.esEndpoint.get}${Configuration.esIndex.get}_count"

      val response = if (message.mins.isEmpty) {
        val body = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"docs.text\":\"" + message.searchTerm + "\"}}]}}}"
        val jsonResponse = Utils.executePost(urlStr, body)
        val count = jsonResponse \ "count"
        List(Tweets("all", count.toString().toLong))
      } else {
        message.mins.map { min =>
          {
            val body = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"docs.text\":\"" + message.searchTerm + "\"}},{\"range\":{\"docs.date\":{\"gt\":\"now-" + min + "m\",\"to\":\"now\"}}}]}}}"
            val jsonResponse = Utils.executePost(urlStr, body)

            val count = jsonResponse \ "count"
            Tweets(min + "mins", count.toString().toLong)
          }
        }
      }

      currentSender ! response
    }
  }
}