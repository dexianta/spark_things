package com.dexian


import java.sql.{Date, Timestamp}
import java.time.{Duration, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.github.javafaker.Faker
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

/**
 * @author ${user.name}
 */


object utils {
  def get_random_date_within(start: LocalDateTime, end: LocalDateTime): Date = {
    val random = new Random()
    val duration = Duration.between(start, end).toDays.toInt
    val rand_day = random.nextInt(duration)
    Date.valueOf(start.plusDays(rand_day).toLocalDate)
  }

  case class salesRecords(date: Date,
                          name: String,
                          address: String,
                          phoneNumber: String,
                          currency: String,
                          code: String,
                          amount: Float)

  def get_one_random_record(faker: Faker, start: LocalDateTime, end: LocalDateTime): salesRecords = {
    salesRecords(
      get_random_date_within(start, end),
      faker.name().fullName(),
      faker.address().fullAddress(),
      faker.phoneNumber().phoneNumber(),
      faker.currency().code(),
      faker.code().isbn13(),
      Random.nextInt(10000000) + Random.nextFloat()
    )
  }

  def get_random_records(faker: Faker, start: LocalDateTime, end: LocalDateTime, num_of_records: Int): Seq[salesRecords] = {
    (0 to num_of_records).par.map(_ => get_one_random_record(faker, start, end)).seq
  }

  def create_random_records_save_csv(spark: SparkSession, num_of_rows: Int):Unit = {
    import spark.implicits._
    val formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
    val start_date = LocalDateTime.parse("01/03/2016 00:00:00", formatter)
    val end_date = LocalDateTime.parse("01/09/2019 00:00:00", formatter)

    val faker = new Faker()
    val records = utils.get_random_records(faker, start_date, end_date, num_of_rows)
    val df = spark.createDataset(records) //.toDF("date", "name", "address", "phoneNumber", "currency", "code", "amount")

    df.write.option("header", "true").mode(SaveMode.Overwrite).csv("/home/dexianta/spark_things/file_things/data/sales/")
  }
}

object MainEntry extends App {
  val spark = SparkSession.builder().appName("files").master("local[*]").getOrCreate()
  utils.create_random_records_save_csv(spark, 5e6.toInt)
  val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/dexianta/spark_things/file_things/data/sales/")
  df.write.mode(SaveMode.Overwrite).format("avro").save("/home/dexianta/spark_things/file_things/data/sales_avro/")
  df.write.mode(SaveMode.Overwrite).format("parquet").save("/home/dexianta/spark_things/file_things/data/sales_parquet/")

//  df.printSchema()
//  df.show()


  println("hello, you've succeed")
}