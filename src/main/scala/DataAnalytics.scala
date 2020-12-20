import org.apache.spark.SparkContext

class DataAnalytics {
  def countDifferentAuthors(context: SparkContext, outlays: Array[String]) = {
    val authors = context.parallelize(outlays.map(outlay => outlay))
    //TODO: Implement logic
  }

  def countWords(context: SparkContext, outlays: Array[String]) = {
    //TODO: Implement logic
  }

  def countDates(context: SparkContext, outlays: Array[String]) = {
    //TODO: Implement logic
  }

  def countFormats(context: SparkContext, outlays: Array[String]) = {
    //TODO: Implement logic
  }

  def countResNames(context: SparkContext, outlays: Array[String]) = {
    //TODO: Implement logic
  }

}
