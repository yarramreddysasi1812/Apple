import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by Sasidhar().
 * yarramreddysasi1812@gmail.com
 * +91 8790908717
 */
object AddDisplay {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Softwares\\hadoop-2.7.2_winbinariesx64")
    val spark = SparkSession
      .builder()
      .appName("Java Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate();

    val idf = spark.read
      .format("csv")
      .option("header", "true")
      .load("D:\\WorkSpace\\IdeaProjects\\Apple\\src\\Resources\\input.csv")
      .withColumn("timestamp", to_timestamp(col("timestamp"), "HH:mma MMM d"))
    idf.printSchema()
    idf.show()

    val noDistinctAds = idf.select("Ad_ID").distinct().collect().length
    print(s"n_distinct_ads = ${noDistinctAds}")
    def getMin(in: Seq[Long]) = in.min
    def getMinTSUDF = udf(getMin _)
    def getMax(a: Seq[Long]) = a.max
    def getMaxTSUDF = udf(getMax _)
    val windowSpec = Window
      .orderBy("timestamp")
      .rowsBetween(Window.currentRow, noDistinctAds - 1) // change 3 to n_distinct_ads
    val withAdsSet =
      idf
        .withColumn("ads_displayed", collect_set("Ad_ID").over(windowSpec))
        .withColumn("ts_long", col("timestamp").cast("long"))
        .withColumn("timestamp_set", collect_set("ts_long").over(windowSpec))
        .withColumn(
          "max_ts",
          getMaxTSUDF(col("timestamp_set")).cast("timestamp")
        )
        .withColumn(
          "min_ts",
          getMinTSUDF(col("timestamp_set")).cast("timestamp")
        )
    val withFlag =
      withAdsSet.withColumn(
        "flag",
        when(size(col("ads_displayed")) === noDistinctAds, 1)
          .otherwise(0)
      )
    withFlag.show()
    withFlag.printSchema()
    def sec_to_time_func =
      (s: Long) => ((s / 3600L) + ":" + (s / 60L) + ":" + (s % 60L))
    def sec_to_time = udf(sec_to_time_func)
    val filteredDF = withFlag
      .filter(col("flag") === 1)
      .withColumn(
        "delta",
        sec_to_time(
          unix_timestamp(col("max_ts")) - unix_timestamp(col("min_ts"))
        )
      )
    filteredDF.sort(col("delta").asc).show(1)
  }
}
