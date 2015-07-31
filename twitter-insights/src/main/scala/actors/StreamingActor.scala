package actors

import akka.actor.Actor
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
import java.text.SimpleDateFormat
import customs.Utils


/**
 * Created by emaorhian
 */
class StreamingActor(ssc: StreamingContext) extends Actor {

  override def receive = {

    case message: ProcessTwitterStream => {
      Configuration.log4j.info(s"[StreamingActor]: starting processing twitter stream")

      val stream = TwitterUtils.createStream(ssc, None)

      val statusesShort = stream.map(status => {
        
        Map("tweepId" -> status.getUser().getId(),
          "date" -> status.getCreatedAt(),
          "text" -> status.getText(),
          "tweetId" -> status.getId(),
          "sentiment" -> Utils.executeGetSentiment(Configuration.sentimentUrl.get, status.getText(), Configuration.sentimentApikey.get, "json"))
      })

      statusesShort.foreachRDD(rdd => rdd.saveToEs(s"${Configuration.esIndex.get}/docs"))

      ssc.start()
      ssc.awaitTermination()

    }
  }
}