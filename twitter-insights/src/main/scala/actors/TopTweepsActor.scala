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
import scalaj.http.Http
import messages.GetTopTweeps
import customs.Utils
import data.Tweeps

/**
 * Created by emaorhian
 */

class TopTweepsActor extends Actor {

  override def receive = {

    case message: GetTopTweeps => {

      Configuration.log4j.info(s"[TopTweepsActor]: Counting tweets for serch term ${message.searchTerm} for the last ${message.mins} mins")
      
      val currentSender = sender
      val result = message.mins.map { min =>
        {
           val body = "{\"query\": {\"bool\": {\"must\": [{\"term\": {\"docs.text\": \"and\"}}," +
        "{\"range\": {\"docs.date\": {\"from\": \"now-100m\",\"to\": \"now\"}}}]}}," +
        "\"aggs\": {\"group_by_tweepId\": {\"terms\": {\"field\": \"tweepId\"}}}}"
          val urlStr = s"${Configuration.esEndpoint.get}${Configuration.esIndex.get}_search"
          val jsonResponse = Utils.executePost(urlStr, body)
          val tweepIds = jsonResponse \ "aggregations" \ "group_by_tweepId" \ "buckets" \\ "key"
          val scores = jsonResponse \ "aggregations" \ "group_by_tweepId" \ "buckets" \\ "doc_count"
          val tweepsArray = tweepIds.map(jsV => jsV.toString().drop(1).dropRight(1)).toArray
          val docCountArray = scores.map(jsV => jsV.toString()).toArray
          val zipped = tweepsArray zip docCountArray
          Tweeps(min + "mins", zipped)
        }
      }
      currentSender ! result
      
    }
  }
}