import edu.umd.cloud9.collection.XMLInputFormat
import edu.umd.cloud9.collection.wikipedia.WikipediaPage
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.sql.{Dataset, SparkSession}

class AssembleDocumentTermMatrix(private val spark: SparkSession) extends Serializable {

  import spark.implicits._

  /**
    * Returns a (title, content) pair.
    */
  def wikiXmlToPlainText(pageXml: String): Option[(String, String)] = {
    val page = new EnglishWikipediaPage()

    // Wikipedia has updated their dumps slightly since Cloud9 was written, so this hacky replacement is sometimes
    // required to get parsing to work.
    val hackedPageXml = pageXml.replaceFirst(
      "<text xml:space=\"preserve\" bytes=\"\\d+\">", "<text xml:space=\"preserve\">")

    WikipediaPage.readPage(page, hackedPageXml)
    if (page.isEmpty || !page.isArticle || page.isRedirect || page.isDisambiguation ||
      page.getTitle.contains("(disambiguation)")) {
      None
    } else {
      Some((page.getTitle, page.getContent))
    }
  }

  def parseWikipediaDump(path: String): Dataset[(String, String)] = {
    val conf = new Configuration()
    conf.set(XMLInputFormat.START_TAG_KEY, "<page>")
    conf.set(XMLInputFormat.END_TAG_KEY, "</page>")
    val kvs = spark.sparkContext.newAPIHadoopFile(path, classOf[XMLInputFormat], classOf[LongWritable],
      classOf[Text], conf)
    val rawXmls = kvs.map(_._2.toString).toDS()

    rawXmls.filter(_ != null).flatMap(wikiXmlToPlainText)
  }

}
