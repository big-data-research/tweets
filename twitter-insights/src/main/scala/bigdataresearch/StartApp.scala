package bigdataresearch

import java.net.InetAddress
import akka.pattern.ask
import scala.collection.JavaConverters._
import com.typesafe.config.Config
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.log4j.Logger
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.Timeout
import customs.CORSDirectives
import messages._
import spray.http.{ StatusCodes, HttpHeaders, HttpMethods, MediaTypes }
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import spray.routing.directives.ParamDefMagnet.apply
import scala.util.{ Failure, Success, Try }
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.duration.Duration._
import spray.routing.Route
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import spray.httpx.marshalling.Marshaller
import spray.http.ContentTypes
import spray.http.StatusCodes.ClientError
import spray.routing._
import Directives._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.FileOutputCommitter
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.fs.Path
import scala.collection.immutable.HashMap
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.elasticsearch.spark._
import actors.StreamingActor
import akka.actor._
import actors.RelevantTermsActor
import spray.http._
import spray.httpx.marshalling._
import spray.json.DefaultJsonProtocol.arrayFormat
import spray.json.JsonFormat
import data.Terms
import java.net.InetAddress
import com.typesafe.config.Config
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.log4j.Logger
import com.google.gson.Gson
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.Timeout
import customs.CORSDirectives
import messages._
import spray.http.{ StatusCodes, HttpHeaders, HttpMethods, MediaTypes }
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.json.DefaultJsonProtocol._
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import spray.routing.directives.ParamDefMagnet.apply
import scala.util.{ Failure, Success, Try }
import org.apache.spark.SparkConf
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.duration.Duration._
import spray.routing.Route
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import spray.json.RootJsonWriter
import spray.json.JsonPrinter
import spray.json.PrettyPrinter
import spray.httpx.marshalling.Marshaller
import spray.http.ContentTypes
import actors.TweetCountActor
import actors.TopTweepsActor
import data.Tweets
import data.Tweeps
import data.Sentiments
import actors.SentimentActor

/**
 * Created by emaorhian
 */

object StartApp extends App with SimpleRoutingApp with CORSDirectives {

  implicit val timeout = Timeout(Configuration.timeout.toInt)
  implicit val spraySystem: ActorSystem = ActorSystem("spraySystem")

  val sparkStreamingContext = createSparkStreamingContext()
  val streamingActor = spraySystem.actorOf(Props(new StreamingActor(sparkStreamingContext)), "streamingActor")
  val tweetsCountActor = spraySystem.actorOf(Props(new TweetCountActor), "tweetsCountActor")
  val relevantTermsActor = spraySystem.actorOf(Props(new RelevantTermsActor), "relevantTermsActor")
  val topTweepsActor = spraySystem.actorOf(Props(new TopTweepsActor), "topTweepsActor")
  val sentimentsActor = spraySystem.actorOf(Props(new SentimentActor), "sentimentsActor")

  def getOptionsRoute: Route = options {
    corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
      complete {
        "OK"
      }
    }
  }

  def indexRoute: Route = path("index") {
    get {

      corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
        complete {
          "Twitter insights is up and running!"
        }
      }

    } ~ getOptionsRoute
  }

  def insightsRoute: Route = pathPrefix("tweets") {
    get {
      path(Segment) { term =>
        parameterMultiMap { mins =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            {
              validateCondition(term != null && !term.trim.isEmpty, Configuration.SEARCH_TERM_MISSING, StatusCodes.BadRequest) {
                respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                  Configuration.log4j.info(s"Search term : $term, interval : $mins")
                  val future = ask(tweetsCountActor, new CountTweets(term, mins.getOrElse("mins", List.empty).map(_.toInt)))

                  future.map {
                    case result: List[Tweets] => ctx.complete(StatusCodes.OK, result)
                    case _                              => ctx.complete(StatusCodes.InternalServerError, "Something is wrong")
                  }
                }
              }
            }
          }
        }
      }
    } ~ getOptionsRoute

  } ~
    pathPrefix("terms") {
      get {
        path(Segment) { term =>
          parameterMultiMap { params =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              {
                validateCondition(term != null && !term.trim.isEmpty, Configuration.SEARCH_TERM_MISSING, StatusCodes.BadRequest) {
                  respondWithMediaType(MediaTypes.`application/json`) { ctx =>

                    val nb = params.getOrElse("nb", List("10")).map(_.toInt)
                    val mins = params.getOrElse("mins", List("1", "5", "15")).map(_.toInt)
                    Configuration.log4j.info(s"Search term : $term, interval : ${nb(0)}")
                    val future = ask(relevantTermsActor, new GetFrequentTerms(term, nb(0), mins))

                    future.map {

                      case result: List[Terms] => ctx.complete(StatusCodes.OK, result)
                      case _                               => ctx.complete(StatusCodes.InternalServerError, "Something is wrong")
                    }
                  }
                }
              }
            }
          }
        }
      } ~ getOptionsRoute

    } ~
    pathPrefix("tweeps") {
      get {
        path(Segment) { term =>
          parameterMultiMap { mins =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            {
              validateCondition(term != null && !term.trim.isEmpty, Configuration.SEARCH_TERM_MISSING, StatusCodes.BadRequest) {
                respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                  Configuration.log4j.info(s"Search term : $term, interval : $mins")
                  val future = ask(topTweepsActor, new GetTopTweeps(term, mins.getOrElse("mins", List.empty).map(_.toInt)))

                  future.map {
                    case result: List[Tweeps] => ctx.complete(StatusCodes.OK, result)
                    case _                              => ctx.complete(StatusCodes.InternalServerError, "Something is wrong")
                  }
                }
              }
            }
          }
        }
        }
      } ~ getOptionsRoute

    } ~
    pathPrefix("sentiment") {
      get {
        path(Segment) { term =>
           parameterMultiMap { mins =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            {
              validateCondition(term != null && !term.trim.isEmpty, Configuration.SEARCH_TERM_MISSING, StatusCodes.BadRequest) {
                respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                  Configuration.log4j.info(s"Search term : $term, interval : $mins")
                  val future = ask(sentimentsActor, new GetSentiment(term, mins.getOrElse("mins", List.empty).map(_.toInt)))

                  future.map {
                    case result: List[Sentiments] => ctx.complete(StatusCodes.OK, result)
                    case _                              => ctx.complete(StatusCodes.InternalServerError, "Something is wrong")
                  }
                }
              }
            }
          }
        }
        }
      } ~ getOptionsRoute
    }

  //************** Start processing twitter stream
  streamingActor ! ProcessTwitterStream()

  //**************Start spary server
  startServer(interface = Configuration.serverInterface.getOrElse(InetAddress.getLocalHost().getHostName()),
    port = Configuration.webServicesPort.getOrElse("8080").toInt) {
      pathPrefix("twitter-insights") {
        indexRoute ~ insightsRoute
      }
    }

  def validateCondition(condition: Boolean, message: String, rejectStatusCode: ClientError): Directive0 = {
    if (condition == false) {
      complete(rejectStatusCode, message)
    }
    pass
  }

  def createSparkStreamingContext(): StreamingContext = {
    System.setProperty("twitter4j.oauth.consumerKey", Configuration.twitterConsumerKey.get)
    System.setProperty("twitter4j.oauth.consumerSecret", Configuration.twitterConsumerSecret.get)
    System.setProperty("twitter4j.oauth.accessToken", Configuration.twitterAccessToken.get)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Configuration.twitterAccessTokenSecret.get)

    val sparkConf = new SparkConf()
      .setMaster(Configuration.sparkMaster.getOrElse("local[2]"))
      .setAppName("twitter-insights")
      .setSparkHome(Configuration.sparkHome.getOrElse("/Users/user"))
      .setJars(Seq(Configuration.jarPath.getOrElse("twitter-insights.jar")))
      .set("spark.ui.port", "5555")
      .set("es.index.auto.create", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(Configuration.sparkBatchDuration.getOrElse("180").toInt))
    ssc
  }
}

object Configuration {

  import com.typesafe.config.ConfigFactory

  val log4j = Logger.getLogger(StartApp.getClass())

  private val conf = ConfigFactory.load
  conf.checkValid(ConfigFactory.defaultReference)

  val appConf = conf.getConfig("appConf")
  val sparkConf = conf.getConfig("sparkConf")
  val twitterConf = conf.getConfig("twitterConf")

  //twitter configuration
  val twitterConsumerKey = getStringConfiguration(twitterConf, "twitter-consumer-key")
  val twitterConsumerSecret = getStringConfiguration(twitterConf, "twitter-consumer-secret")
  val twitterAccessToken = getStringConfiguration(twitterConf, "twitter-access-token")
  val twitterAccessTokenSecret = getStringConfiguration(twitterConf, "twitter-access-token-secret")

  // spark configuration
  val sparkMaster = getStringConfiguration(sparkConf, "spark.master")
  val sparkHome = getStringConfiguration(sparkConf, "spark.home")
  val sparkBatchDuration = getStringConfiguration(sparkConf, "spark.batch.duration")

  //app configuration
  val serverInterface = getStringConfiguration(appConf, "server.interface")
  val applicationName = getStringConfiguration(appConf, "application.name")
  val webServicesPort = getStringConfiguration(appConf, "web.services.port")
  val timeout = getStringConfiguration(appConf, "timeout").getOrElse("10000").toInt
  val corsFilterAllowedHosts = getStringConfiguration(appConf, "cors-filter-allowed-hosts")
  val jarPath = getStringConfiguration(appConf, "jar-path")
  
  val sentimentUrl=getStringConfiguration(appConf, "sentiment-url")
  val sentimentApikey=getStringConfiguration(appConf, "sentiment-apikey")
  
  val esEndpoint = getStringConfiguration(appConf, "es-endpoint")
  val esIndex= getStringConfiguration(appConf, "es-index")

  val SEARCH_TERM_MISSING = "The search term is missing!"

  def getStringConfiguration(configuration: Config, configurationPath: String): Option[String] = {
    if (configuration.hasPath(configurationPath)) Option(configuration.getString(configurationPath).trim) else Option(null)
  }
}