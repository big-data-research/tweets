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
import messages.GetSentiment
import data.Sentiments

/**
 * Created by emaorhian
 */

class SentimentActor extends Actor {

  override def receive = {

    case message: GetSentiment => {

      Configuration.log4j.info(s"[SentimentActor]: Retrieving sentiment in tweets for search term ${message.searchTerm} for the last ${message.mins} mins")
      val currentSender = sender

      val urlStr = s"${Configuration.esEndpoint.get}${Configuration.esIndex.get}_search"

      val result = message.mins.map { min =>
        {
           val body = "{\"query\": {\"bool\": {\"must\": [{\"term\": {\"docs.text\": \""+message.searchTerm+"\"}}," +
                    "{\"range\": {\"docs.date\": {\"from\": \"now-"+ min +"m\",\"to\": \"now\"}}}]}},"+
                    "\"fields\" : [\"sentiment\", \"tweetId\"]}"
          val urlStr = "http://localhost:9200/tweets/_search"
          val jsonResponse = Utils.executePost(urlStr, body)
          println("!!!!! " + body)
          println(jsonResponse.toString())
          val tweetIds = jsonResponse \\ "tweetId"
          val sentiment = jsonResponse \\ "sentiment"
          val tweetIdsArray = tweetIds.map(jsV => jsV.toString()).toArray
          val sentimentArray = sentiment.map(jsV => jsV.toString().drop(1).dropRight(1)).toArray
          val zipped = tweetIdsArray zip sentimentArray
          Sentiments(min + "mins", zipped)
        }
      }

      currentSender ! result
    }
  }
}