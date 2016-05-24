import java.util.{Calendar, Date}

import org.apache.spark.ml.feature._
import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.TwitterFactory

object TwitterPopularTags {
  def main(args: Array[String]) {

    // Create the context with a 2 second batch size
    val conf = new SparkConf().setAppName("TwitterPopularTags")
    val spark = new SparkContext(conf)
    object auth{
      val config = new twitter4j.conf.ConfigurationBuilder()
        .setOAuthConsumerKey("kIC3vbMTL4zcrcF8O7ovnZRSf")
        .setOAuthConsumerSecret("EnhDOJk55HJosMCKLgbHXqejuw4GL3sQiwhEd4U4hf6Wd3RJaP")
        .setOAuthAccessToken("223302713-Vf61OR8IzoDLW9gtKhF6FvGrHsMjlih5O8XXYcdk")
        .setOAuthAccessTokenSecret("pTWlURDedsgrDNnz34g4pynxMZXkYjIXof4yzaY9A6V36")
        .build
    }

    val tokenizer = {

    }
    val twitter_auth = new TwitterFactory(auth.config)
    val a = new twitter4j.auth.OAuthAuthorization(auth.config)
    val atwitter =  twitter_auth.getInstance(a).getAuthorization()
    //val filters = Array("#GameofThrones","#hodor")
    val filters = Array("#rio2016","#roadtorio","Brazil", "Olympic")

    val ssc = new StreamingContext(spark, Seconds(120))
    val sqlContext = new SQLContext(spark)

    val tweets = TwitterUtils.createStream( ssc, Some( atwitter ), filters)

    //val statuses = tweets.filter(status => status.getLang().equalsIgnoreCase("en"))
    val statuses = tweets.filter(status => status.getLang().equalsIgnoreCase("en")).map(status => status.getText())
    var ventana=0
    //iterando
    statuses.foreachRDD((rdd: RDD[String]) => {
      // Get the singleton instance of SQLContext

      ventana=ventana+1
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      val hoy = Calendar.getInstance.getTime
      import sqlContext.implicits._
      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w,hoy.toString,ventana)).toDF()
      // Register as table

      wordsDataFrame.registerTempTable("tweets")

      // Do word count on table using SQL and print it
      var clean = Array("brazil", "rt","#rio2016")
      val stemmed = new Stemmer().setInputCol("word").setOutputCol("tweets").setLanguage("English")
      val tweetsStemmer= stemmed.transform(wordsDataFrame)
      var transformar= new TweetTransformer().setInputCol("tweets").setOutputCol("tweettrans")
      var tweetTrans =transformar.transform(tweetsStemmer)
      val tokenizer = new RegexTokenizer().setInputCol("tweettrans").setOutputCol("tokens").setToLowercase(true)
      val tweetsToken = tokenizer.transform(tweetTrans)
      val stopWords = new StopWordsRemover().setInputCol("tokens").setOutputCol("corpus")
      stopWords.setStopWords(clean)
      val corpus=stopWords.transform(tweetsToken)
      corpus.show()

      val hashingTF = new HashingTF().setInputCol("corpus").setOutputCol("rawFeatures").setNumFeatures(1000)
      val featurizedData = hashingTF.transform(corpus)
      val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
      val idfModel = idf.fit(featurizedData)
      val rescaledData = idfModel.transform(featurizedData)
      //clustering
      val kmeans = new KMeans()
        .setK(3)
        .setFeaturesCol("features")
        .setPredictionCol("cluster")
      val model = kmeans.fit(rescaledData)
      val predictionResult = model.transform(rescaledData)
      // Shows the result
      var resultados=predictionResult.select("corpus","cluster","date","i")



      val saveConfig = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "clusters", Collection ->"cluster", SplitKey -> "_id"))
      resultados.saveToMongodb(saveConfig.build)

      // Evaluate clustering by computing Within Set Sum of Squared Errors
      //val WSSSE = model.computeCost(parsedData)
      //println("Within Set Sum of Squared Errors = " + WSSSE)
    })


    ssc.start()
    ssc.awaitTermination()
  }




}
case class Record(word: String,date: String, i: Int)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {
  @transient  private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}


