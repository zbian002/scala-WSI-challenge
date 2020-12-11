package org.example

import net.liftweb.json.Serialization.write
import net.liftweb.json.{DefaultFormats, _}
import play.api.libs.json.Json

import scala.collection.mutable.ListBuffer

case class Products(id: String, interest: Float)

case class VisitsAndProducts(visitorId: String, products: List[NewProducts])

case class NewProducts(id: String, name: String, interest: Double)

object StoreVisitors {

  def main(args: Array[String]): Unit = {
    updateJson
  }

  def updateJson: Seq[String] = {
    implicit val formats = DefaultFormats

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

    var output = ListBuffer[String]()

    for (visitdata <- visitsData) {
      val json = parse(visitdata)

      val json1 = Json.parse(visitdata)

      val visitorId = (json1 \ "visitorId").get.toString().split("\"")

      val elements = (json \ "products").children

      var list = ListBuffer[NewProducts]()

      for (acct <- elements) {
        val m = acct.extract[Products]
        val name = productIdToNameMap.getOrElse(m.id, None)

        val newProducts = NewProducts(m.id, name.toString,
          BigDecimal(m.interest).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)

        list += newProducts
      }

      val visitsAndProducts = VisitsAndProducts(visitorId(1), list.toList)

      val jsonString = write(visitsAndProducts)

      output += jsonString
    }

    output.toSeq
  }
}