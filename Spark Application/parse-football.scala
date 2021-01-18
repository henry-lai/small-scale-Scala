import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

object AssignmentPart2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Assignment2")
      .getOrCreate();

    // remove some of the INFO msg
    spark.sparkContext.setLogLevel("ERROR")

    //Read in csv file into Dataframe
    val df = spark.read.option("header", true).csv("scores/*")

    //Change some of the FTHG  FTAG column types into integer
    val df2 = df.select(
      df("HomeTeam").as("HomeTeam"),
      df("AwayTeam").as("AwayTeam"),
      df("FTHG").cast(IntegerType).as("FTHG"),
      df("FTAG").cast(IntegerType).as("FTAG"))

    //groups away goals and home goals by Team
    val df3 : sql.DataFrame = df2.groupBy("HomeTeam").sum("FTHG")
    val df4 : sql.DataFrame = df2.groupBy("AwayTeam").sum("FTAG")

    //Group FTAG and FTHG columns by their Teams
    val df5 : sql.DataFrame = df3.join(df4,df3.col("HomeTeam").equalTo(df4("AwayTeam")))

    //Combine sum(FTHG) and sum(FTAG) columns to get total goals scored
    val df6 : sql.DataFrame = df5.groupBy("HomeTeam").sum()

    //add the AwayGoal and HomeGoal Column together to get total goal
    val df7 : sql.DataFrame = df6.withColumn("Total Goals", col("sum(sum(FTHG))")+ col("sum(sum(FTAG))"))

    //Final Result!!!!
    val df8 : sql.DataFrame = df7.select("HomeTeam","Total Goals")

    //Renaming a column header
    val df9 : sql.DataFrame = df8.withColumnRenamed("HomeTeam", "Team")

    //Print Dataframe and schema into console
    df9.show()
    df9.printSchema()
  }
}