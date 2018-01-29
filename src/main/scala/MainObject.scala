import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, SparkSession}

object MainObject {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).master("local").getOrCreate()
    val assembleMatrix = new AssembleDocumentTermMatrix(spark)
    import assembleMatrix._
    val docTexts: Dataset[(String, String)] = parseWikipediaDump("hdfs://madison:32701/wiki/")

    docTexts.show(2)

  }

}
