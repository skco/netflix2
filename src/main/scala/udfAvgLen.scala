import org.apache.spark.sql.api.java.{UDF1}

class strLen extends UDF1[String, Int]{
  override def call(title: String): Int = {
    title.length
    }
}

class wordsCount extends UDF1[String, Int]{
  override def call(title: String): Int = {
    title.split(' ').length
  }
}
