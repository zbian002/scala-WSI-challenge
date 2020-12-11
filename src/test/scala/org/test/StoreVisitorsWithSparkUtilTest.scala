package org.test

import net.liftweb.json.{DefaultFormats, parse}
import org.example.{NewProducts, StoreVisitors, StoreVisitorsWithSpark}
import org.junit.Assert.assertEquals
import play.api.libs.json.Json

class StoreVisitorsWithSparkUtilTest {
  @org.junit.Test
  def test1(): Unit = {

    implicit val formats = DefaultFormats

    val visitorsAndProducts = StoreVisitorsWithSpark.updateJson

    val visitorsAndProduct1 = visitorsAndProducts(0)

    val json1 = Json.parse(visitorsAndProduct1)

    val visitorId = (json1 \ "visitorId").get.toString().split('"')(1)

    assertEquals(visitorId, "v1")

    val json = parse(visitorsAndProduct1)

    val elements = (json \ "products").children

    val m = elements(0).extract[NewProducts]

    assertEquals(m.id, "i1")

    assertEquals(m.name, "Nike Shoes")

    assertEquals(m.interest, 0.68, 2)

    val n = elements(1).extract[NewProducts]

    assertEquals(n.id, "i2")

    assertEquals(n.name, "Umbrella")

    assertEquals(n.interest, 0.42, 2)
  }

  @org.junit.Test
  def test2(): Unit = {

    implicit val formats = DefaultFormats

    val visitorsAndProducts = StoreVisitors.updateJson

    val visitorsAndProduct2 = visitorsAndProducts(1)

    val json1 = Json.parse(visitorsAndProduct2)

    val visitorId = (json1 \ "visitorId").get.toString().split('"')(1)

    assertEquals(visitorId, "v2")

    val json = parse(visitorsAndProduct2)

    val elements = (json \ "products").children

    val m = elements(0).extract[NewProducts]

    assertEquals(m.id, "i1")

    assertEquals(m.name, "Nike Shoes")

    assertEquals(m.interest, 0.78, 2)

    val n = elements(1).extract[NewProducts]

    assertEquals(n.id, "i3")

    assertEquals(n.name, "Jeans")

    assertEquals(n.interest, 0.11, 2)
  }
}
