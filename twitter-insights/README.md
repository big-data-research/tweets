#TWITTER INSIGHTS

This project is a POC that reads the twitter stream using Spark Streaming, saves some fields in elastic search and exposes a set of apis that offer insights about the data.

## Building

You need to have Maven installed.
To build, follow the below steps:

    cd twitter-insights
    mvn clean install -DskipTests
    cd twitter-insights/target
    tar -zxvf twitter-insights.tar.gz

## Configuration

To configure the project edit the following file: twitter-insights/conf/application.conf.

You have to set :
	
	* your twitter secret access keys, 
	* spark master, 
	* spark streaming batch duration, 
	* the path to the target/twitter-insights.jar, 
	* the elasticsearch url, 
	* index name
	* the Text API api key

Important:
	For sentiment analysis the Text API online service so you will have to obtain a free apiKey registering here: http://www.alchemyapi.com/api/register.html

The existing application.conf file is configured for spark in local mode and elasicsearch on localhost.

## Run 

After editing all the configuration files the application can be run in the following manner:

    1. go where you decompressed the tar.gz file:
       cd twitter-insights
    2. run Jaws:
       nohup bin/bin/start-twitter-insights.sh &

## Apis usage examples

###Tweets count

This api answers to the following questions: 

1. "What is the total count of tweets matching the search term seen so far?"
2.  "How many tweets containing the search term were there in the last 1, 5 and 15 minutes?"


		http://localhost:9080/twitter-insights/tweets/{searchTerm}?mins=1&mins=10&mins=15

Parameters:

	* searchTerm : the term for search
	* mins : tweets time interval until now

If mins is not specified all the tweest will be considered so far

###Frequent terms

This api answers to the following question: 
"What are the ten most frequent terms (excluding the search term) that appear in tweets containing the search term over the last 1, 5 and 15 minutes?"


	http://localhost:9080/twitter-insights/terms/{searchTerm}?nb=10&mins=1&mins=10&mins=15

Parameters:

	* searchTerm : the term for search
	* mins : tweets time interval until now
	* nb: the number of the most frequent terms

If mins is not specified, a default of  1, 5 and 10 mins will be considered

###Top tweeps (Twitter users)

This api answers to the following question: 
"Within tweets matching the search term, who were the top ten tweeps (Twitter users) who tweeted the most in the last 1, 5 and 15 minutes?"


	http://localhost:9080/twitter-insights/tweeps/{searchTerm}?mins=1&mins=10&mins=100

Parameters:

	* searchTerm : the term for search
	* mins : tweets time interval until now

If mins is not specified, a default of  1, 5 and 10 mins will be considered

###Tweets sentiment

This api answers to the following question: 
"What is the sentiment of tweets matching the search term over the last 1, 5 and 15 minutes?"


	http://localhost:9080/twitter-insights/sentiment/{searchTerm}?mins=1&mins=10&mins=100

Parameters:

	* searchTerm : the term for search
	* mins : tweets time interval until now

If mins is not specified, a default of  1, 5 and 10 mins will be considered

 





   