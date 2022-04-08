import org.apache.spark.sql.{SparkSession, hive}

import java.net.URL
import com.rometools.rome.feed.synd.SyndFeed
import com.rometools.rome.io.SyndFeedInput
import com.rometools.rome.io.XmlReader

//import javax.sql.rowset.spi.XmlReader
import scala.collection.JavaConversions.asScalaBuffer


object Project_1 {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    //System.setProperty("hadoop.home.dir", "C:\\winutils")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR") //This doesn't seem to do much but whatever.

    val feedUrl = new URL("https://news.google.com/rss/search?q=politics&hl=en-US&gl=US&ceid=US:en")
    val input = new SyndFeedInput
    val feed: SyndFeed = input.build(new XmlReader(feedUrl))

    var key = 0
    spark.sql("DROP TABLE IF EXISTS newsfeed")
    spark.sql("CREATE TABLE IF NOT EXISTS newsfeed (key INT, title STRING, uri STRING, date STRING, link STRING) USING hive")
    val entries = (feed.getEntries).toVector
    for (entry <- entries){
      val title = entry.getTitle.replaceAll("'", "x").replaceAll(";", "X")
      val uri = entry.getUri.replaceAll("'", "x").replaceAll(";", "X")
      val date = entry.getPublishedDate
      entry.getCategories
      val link = entry.getLink.replaceAll("'", "x").replaceAll(";", "X")
      spark.sql(s"INSERT INTO newsfeed VALUES ($key, '$title', '$uri', '$date', '$link')")
      key = key + 1
    }

    var continue = true
    while(continue){
      println("SQL or EXIT")
      var newInput = scala.io.StdIn.readLine()
      if(newInput.toLowerCase() == "exit"){
        continue = false
      }
      else
        {
          var newSQLInput = scala.io.StdIn.readLine()
          println(newSQLInput)
          println("Show?")
          var show = scala.io.StdIn.readLine()
          if(show.toLowerCase == "y" | show.toLowerCase == "yes"){
            spark.sql( s"$newSQLInput").show(20, 200) //This would be a fantastic spot for exception handling.
          }
          else {
            spark.sql( s"$newSQLInput")
          }
        }
    }
    println("Exited")
    spark.close()
  }
}
/*
SELECT * FROM newsfeed

Most recently updated
SELECT * FROM newsfeed ORDER BY uri ASC

Least recently updated (still pretty fresh)
SELECT * FROM newsfeed ORDER BY uri DESC

Longest title?
SELECT title FROM newsfeed ORDER BY LENGTH(title) DESC

Shortest title?
SELECT title FROM newsfeed ORDER BY LENGTH(title) ASC

How many mention Ukraine?
SELECT COUNT(title) FROM newsfeed WHERE title LIKE '%Ukraine%'

How many mention Covid?
SELECT COUNT(title) FROM newsfeed WHERE title LIKE '%COVID%'

The ones that mention Florida
SELECT * FROM newsfeed WHERE title LIKE '%Florida%'


*/
