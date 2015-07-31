package customs

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.PostMethod
import play.api.libs.json.Json
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.params.HttpMethodParams
import scala.io.Source

/**
 * Created by emaorhian
 */
object Utils {

  def executePost(urlStr: String, body: String) = {

    val httpClient = new HttpClient()
    val method = new PostMethod(urlStr);
    method.addRequestHeader("Content-Type", "application/json")
    method.setRequestBody(body)
    httpClient.executeMethod(method)
    val response = method.getResponseBodyAsString
    Json.parse(response)
  }

  def executeGetSentiment(urlStr: String, text: String, apiKey: String, outputMode: String) = {
    val encodedText = java.net.URLEncoder.encode(text.replaceAll("\"", ""), "UTF-8")   
    val url = s"$urlStr?text=$encodedText&apikey=$apiKey&outputMode=$outputMode"
    println(url)
    val responseString = Source.fromURL(url).mkString
    val json = Json.parse(responseString)
    val sentiment = json \ "docSentiment" \ "type"
    sentiment.toString().drop(1).dropRight(1)
  }
}