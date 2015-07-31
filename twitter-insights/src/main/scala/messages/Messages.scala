package messages

/**
 * Created by emaorhian
 */
case class ProcessTwitterStream()
case class CountTweets(searchTerm : String, mins : List[Int])
case class GetFrequentTerms(searchTerm : String, termsNumber : Int,  mins : List[Int])
case class GetTopTweeps(searchTerm : String, mins : List[Int])
case class GetSentiment(searchTerm : String, mins : List[Int])
