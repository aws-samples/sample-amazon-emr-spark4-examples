import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.immutable.Set
import scala.collection.JavaConverters._

object Spark3_3_Job {

  val S3_BASE = "s3://<backet name>/data/Spark_4_0_Upgrade"
  val INPUT_PATH = s"$S3_BASE/input/airports.csv"
  val OUTPUT_BASE = s"$S3_BASE/output"
  val SCHEMA_PATH = s"$S3_BASE/schema/airport_schema.json"

  def loadSchemaFromS3(spark: SparkSession, schemaPath: String): StructType = {
    val schemaJson = spark.read.text(schemaPath)
      .collect()
      .map(_.getString(0))
      .mkString("")
    DataType.fromJson(schemaJson).asInstanceOf[StructType]
  }

  def readCsvWithSchema(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .schema(schema)
      .csv(path)
  }

  def writeParquet(df: DataFrame, path: String, partitionCols: Seq[String] = Seq.empty): Unit = {
    val writer = df.write.mode("overwrite")
    if (partitionCols.nonEmpty) writer.partitionBy(partitionCols: _*).parquet(path)
    else writer.parquet(path)
  }

  def writeJson(df: DataFrame, path: String): Unit = {
    df.coalesce(1).write.mode("overwrite").json(path)
  }

  def collectDistinctValues(spark: SparkSession, query: String): Set[String] = {
    // Migration change: The to[Collection] method was replaced by the to(Collection) method.
    spark.sql(query).collect().map(_.getString(0)).to[Set]
  }

  def main(sysArgs: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("airport-data-pipeline").getOrCreate()
    // Migration change: spark.sql.legacy.parquet.int96RebaseModeInRead -> spark.sql.parquet.int96RebaseModeInRead
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")

    // ── Stage 1: Ingest raw data ──
    val airportSchema = loadSchemaFromS3(spark, SCHEMA_PATH)
    val rawDf = readCsvWithSchema(spark, INPUT_PATH, airportSchema)
    rawDf.cache()
    println(s"Ingested ${rawDf.count()} rows")
    rawDf.createOrReplaceTempView("airports_raw")

    // ── Stage 2: Core transformations (original logic preserved) ──
    // Migration change: Value is overflow and fail the job, since ansi is enabled by default
    var df = spark.sql("SELECT *, CAST(build_time AS SMALLINT) as numeric_build_time FROM airports_raw")
    df.show()
    df.createOrReplaceTempView("airports")

    val airportsInUs: Set[String] = collectDistinctValues(spark,
      "SELECT DISTINCT name FROM airports WHERE country='USA'")
    println(s"US airports: $airportsInUs")
    val airportsInUsJava: java.util.Set[String] = airportsInUs.asJava

    // Migration change: CAST on malformed value will fail the job
    df = spark.sql(
      """SELECT *,
        |  CAST(code AS INT) as numeric_code,
        |  labor_costs / employees as labor_cost_per_person
        |FROM airports""".stripMargin)
    df.show()

    // ── Stage 3: Enrichment ──
    df.createOrReplaceTempView("airports_enriched")
    val enrichedDf = spark.sql(
      """SELECT *,
        |  CASE
        |    WHEN labor_cost_per_person < 1000 THEN 'low'
        |    WHEN labor_cost_per_person < 2500 THEN 'medium'
        |    ELSE 'high'
        |  END as cost_tier,
        |  YEAR(build_time) as build_year,
        |  CASE
        |    WHEN employees < 200 THEN 'small'
        |    WHEN employees < 700 THEN 'medium'
        |    ELSE 'large'
        |  END as airport_size
        |FROM airports_enriched""".stripMargin)
    enrichedDf.show()

    // ── Stage 4: Country-level aggregation ──
    enrichedDf.createOrReplaceTempView("airports_final")
    val countryStatsDf = spark.sql(
      """SELECT
        |  country,
        |  COUNT(*) as airport_count,
        |  SUM(employees) as total_employees,
        |  SUM(labor_costs) as total_labor_costs,
        |  ROUND(AVG(labor_cost_per_person), 2) as avg_cost_per_person,
        |  MIN(build_year) as oldest_build_year,
        |  MAX(build_year) as newest_build_year
        |FROM airports_final
        |GROUP BY country
        |ORDER BY total_employees DESC""".stripMargin)
    countryStatsDf.show(50)

    // ── Stage 5: Write outputs ──
    writeParquet(enrichedDf, s"$OUTPUT_BASE/enriched", Seq("country"))
    writeParquet(countryStatsDf, s"$OUTPUT_BASE/country_stats")
    writeJson(countryStatsDf, s"$OUTPUT_BASE/country_stats_json")

    rawDf.unpersist()
    println("Pipeline complete.")
  }
}
