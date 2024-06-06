import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object RoadTraffic extends App {

  // Load the configuration file
  val config = ConfigFactory.load()
  val inputStream = config.getString("Stream.input")
  val checkpointPath = config.getString("Stream.checkpoint")
  val sinkPath = config.getString("Stream.sink")

  // Create the Spark Session
  val boolVal1 = true
  val boolVal2 = true
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Streaming Process Files")
    .config("spark.streaming.stopGracefullyOnShutdown", boolVal1)
    .getOrCreate()

  // To allow automatic schema inference while reading
  spark.conf.set("spark.sql.streaming.schemaInference", boolVal2)

  //todo : make a test unitaire for this function
  def filterTtravelTimeReliability(streamDF: DataFrame) : DataFrame =  {
    return streamDF.filter(col("meanTravelTimeReliability") > 50)
  }

  //todo : make a test unitaire for this function
  def imputationColumnSensCircule(streamDF: DataFrame) : DataFrame = {
    return streamDF.na.fill("Double sens", Seq("sens_circule"))
  }
//todo : make a test unitaire for this function
  val mode: Seq[String] => String = (values: Seq[String]) => {
    if (values.isEmpty) null
    else values.groupBy(identity).mapValues(_.size).maxBy(_._2)._1
  }
  //create a UDF for the mode function
  val modeUDF = udf(mode)

  def myAggregation(streamDF: DataFrame) : DataFrame = {
    val df = streamDF
      .select(col("denomination"),col("datetime"), col("averageVehicleSpeed"), col("travelTime"), col
      ("travelTimeReliability"), col("trafficStatus"), col("vehicleProbeMeasurement"), col("vitesse_maxi"))
      .groupBy(col("denomination"), col("datetime"))
      .agg(
        count("*").alias("nbTroconPerRoad"),
        sum("vehicleProbeMeasurement").alias("nbVehiculePerRoad"),
        mean("averageVehicleSpeed").alias("meanVitessePerRoad"),
        max("vitesse_maxi").alias("vitesseMaximumPerRoad"),
        mean("travelTime").alias("meanTravelTime"),
        mean("travelTimeReliability").alias("meanTravelTimeReliability"),
        collect_list(col("trafficStatus")).as("trafficStatusList")
      ).coalesce(1) 
    // .coalesce(1) : to decrease the nmbre of partitions of the DF to 1

    return df.select(col("denomination"),col("datetime"),col("nbTroconPerRoad"),col("nbVehiculePerRoad"), col
    ("meanVitessePerRoad"), col("vitesseMaximumPerRoad"), col("meanTravelTime"), col("meanTravelTimeReliability"),
      modeUDF(col("trafficStatusList")).as("stateGeneralTrafic"))
  }

  def saveToCSV(df: Dataset[Row], batchId: Long, outputDir: String): Unit = {
    val batchOutputDir = s"$outputDir/batch_$batchId"
    df
      .write
      .option("header", "true")
      .csv(batchOutputDir)
  }
  // Create the json to read from the input directory
  val jsonDF = spark.readStream
    .format("json")
    .option("maxFilesPerTrigger", 1)
    .load(inputStream)


  val StreamDF = jsonDF
    .withColumn("result", explode(col("results")))
    .select(
      col("result.datetime").as("datetime"),
      col("result.predefinedlocationreference").as("predefinedlocationreference"),
      col("result.averagevehiclespeed").as("averagevehiclespeed"),
      col("result.traveltime").as("traveltime"),
      col("result.traveltimereliability").as("traveltimereliability"),
      col("result.trafficstatus").as("trafficstatus"),
      col("result.vehicleprobemeasurement").as("vehicleprobemeasurement"),
      col("result.geo_point_2d").as("Geo Point"),
      col("result.geo_shape.type").as("Geo Shape"),
      col("result.hierarchie").as("hierarchie"),
      col("result.hierarchie_dv").as("hierarchie_dv"),
      col("result.denomination").as("denomination"),
      col("result.insee").as("insee"),
      col("result.sens_circule").as("sens_circule"),
      col("result.vitesse_maxi").as("vitesse_maxi")
    )

  val aggDF = myAggregation(StreamDF)
  val filterDF = filterTtravelTimeReliability(aggDF)
  //
  filterDF
    .writeStream
    .outputMode("complete")
    .option("checkpointLocation", checkpointPath)
    .foreachBatch(saveToCSV(_, _, sinkPath))
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()
    .awaitTermination()
}
