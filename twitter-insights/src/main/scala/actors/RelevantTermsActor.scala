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
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import scalaj.http.Http
import messages.GetFrequentTerms
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.PostMethod
import org.apache.commons.httpclient.methods.RequestEntity
import org.apache.commons.httpclient.methods.StringRequestEntity
import org.jboss.netty.handler.codec.http.multipart.HttpPostBodyUtil
import data.Terms
import customs.Utils

/**
 * Created by emaorhian
 */

class RelevantTermsActor extends Actor {

  override def receive = {

    case message: GetFrequentTerms => {

      Configuration.log4j.info(s"[RelevantTermsActor]: Geting top ${message.termsNumber} relevant terms for serch term ${message.searchTerm}")
      val currentSender = sender
      val result = message.mins.map { min =>
        {
          val body = "{\"query\": {\"bool\": {\"must\": [{\"term\": {\"docs.text\": \"" + message.searchTerm + "\"}},{\"range\": {\"docs.date\": { \"from\": \"now-" + min + "m\",\"to\": \"now\"}}}]}},\"aggs\": {\"termsRel\": {\"terms\": {\"field\": \"text\", \"exclude\": \"" + message.searchTerm + "\", \"size\": \"" + message.termsNumber + "\"}}}}"
          val urlStr = s"${Configuration.esEndpoint.get}${Configuration.esIndex.get}_search"
          val jsonResponse = Utils.executePost(urlStr, body)
          val terms = jsonResponse \ "aggregations" \ "termsRel" \ "buckets" \\ "key"
          val scores = jsonResponse \ "aggregations" \ "termsRel" \ "buckets" \\ "doc_count"
          val termArray = terms.map(jsV => jsV.toString().drop(1).dropRight(1)).toArray
          val docCountArray = scores.map(jsV => jsV.toString()).toArray
          val zipped = termArray zip docCountArray
          Terms(min + "mins", zipped)
        }
      }
      currentSender ! result
    }
  }
}