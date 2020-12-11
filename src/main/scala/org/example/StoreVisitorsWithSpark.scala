package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object StoreVisitorsWithSpark {
  def main(args: Array[String]): Unit = {
    updateJson

    println(updateJson)
  }

  def updateJson: Seq[String] = {
    val spark = SparkSession
      .builder()
      .appName("Scala Challenge")
      .master("local[1]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._

    val rec1: String = """{
        "visitorId": "v1",
        "products": [{
             "id": "i1",
             "interest": 0.68
        }, {
             "id": "i2",
             "interest": 0.42
        }]
      }"""

    val rec2: String = """{
        "visitorId": "v2",
        "products": [{
             "id": "i1",
             "interest": 0.78
        }, {
             "id": "i3",
             "interest": 0.11
        }]
      }"""

    val visitsData: Seq[String] = Seq(rec1, rec2)

    val productIdToNameMap = Map("i1" -> "Nike Shoes", "i2" -> "Umbrella", "i3" -> "Jeans")

    val visitsDataDF = spark.read.json(visitsData.toDS)

    val visitorsAndProduct = visitsDataDF.select($"visitorId", explode($"products").as("products"))
      .withColumn("productId", $"products.id")
      .withColumn("productInterest", $"products.interest")
      .drop("products")

    val productIdToNameMapDF = productIdToNameMap.toSeq.toDF("Id", "productName")

    val visitorsAndProductNameDF = visitorsAndProduct.join(productIdToNameMapDF,
      visitorsAndProduct.col("productId") === productIdToNameMapDF.col("Id"))
      .select($"visitorId", $"productId".as("id"), $"productName".as("name"),
        $"productInterest".as("interest"))
      .withColumn("products", struct($"id", $"name", $"interest"))
      .drop("id").drop("name").drop("interest")
      .groupBy("visitorId").agg(collect_list($"products").as("products"))

    val visitorsAndProductDets = visitorsAndProductNameDF.toJSON.collect().toSeq

    var result = ListBuffer[String]()

    result += visitorsAndProductDets(1)

    result += visitorsAndProductDets(0)

    result.toSeq
  }
}