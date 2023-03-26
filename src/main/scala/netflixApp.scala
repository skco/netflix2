import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes}

object netflixApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("netflix")
      .master("local[*]")
      .getOrCreate()

    val strLen: strLen = new strLen()
    spark.udf.register("strLen",strLen, DataTypes.IntegerType)

    val wordsCount: wordsCount = new wordsCount()
    spark.udf.register("wordsCount", wordsCount, DataTypes.IntegerType)

    val netflixDF:Dataset[Row] = spark
      .read
      .option("header", true)
      .option("delimiter", ",")
      .csv("netflix_titles.csv")
      .na.fill("NULL")

       //part1
      //count all
       println("count:",netflixDF.count())

       //count by type
       netflixDF.groupBy("type").count().show()

       // count by director
       netflixDF
         .groupBy("director")
         .count()
         .sort(col("count").desc)
         .show(numRows= netflixDF.count().toInt) //show all rows

       //count by release_year
       netflixDF
         .groupBy("release_year")
         .count()
         .sort(col("release_year").desc)
         .show(numRows = netflixDF.count().toInt)  //show all rows

       val netflixExplodedOninListedDF: Dataset[Row] = netflixDF
         .withColumn("listed_in", split(col("listed_in"), ","))
         .select(col("show_id"), explode(col("listed_in")))

       netflixExplodedOninListedDF.show(truncate = false,numRows = netflixExplodedOninListedDF.count().toInt)
       netflixExplodedOninListedDF.groupBy("col").count().show()

       netflixExplodedOninListedDF.printSchema()

       //part 2

       val netflixTitleLengthDF: Dataset[Row] = netflixDF
                                                .withColumn("titleLenCharacters",
                                                call_udf("strLen", col("title")))

       val netflixWordCountDF:Dataset[Row] = netflixTitleLengthDF.withColumn("titleLenWords", call_udf("wordsCount", col("title")))

       netflixTitleLengthDF.show()

       netflixTitleLengthDF.select(mean(netflixTitleLengthDF("titleLenCharacters")).as("average title length  characters")).show()
       netflixTitleLengthDF.select(mean(netflixTitleLengthDF("titleLenWords")).as("avg words in title ")).show()

  }
}
