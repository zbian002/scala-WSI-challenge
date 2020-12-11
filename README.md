Visitors of an eCommerce site browse multiple products during their visit. All visit data of a visitor is consolidated in a JSON document containing vistor Id and a list of product Ids, along with an interest attribute containing value of interest expressed by visitor in a product.  Here are two example records - rec1 and rec2 containing visit data of two visitors v1 and v2:
 
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
 
Given the collection of records (visitsData) and a map (productIdToNameMap) of product Ids and their names:

1. Write the code to enrich every record contained in visitsData with the name of the product. The output should be another sequence with all the original JSON documents enriched with product name. Here is the example output.
 
val output: Seq[String] = Seq(enrichedRec1, enrichedRec1)
 
where enrichedRec1 has value -
"""{
    "visitorId": "v1",
    "products": [{
         "id": "i1",
         "name": "Nike Shoes",
         "interest": 0.68
    }, {
         "id": "i2",
         "name": "Umbrella",
         "interest": 0.42
    }]
}"""
 
And enrichedRec2 has value -
"""{
    "visitorId": "v2",
    "products": [{
         "id": "i1",
         "name": "Nike Shoes",
         "interest": 0.78
    }, {
         "id": "i3",
         "name": "Jeans",
         "interest": 0.11
    }]
}"""
 
2. Please write two sets of code - one using only scala (no Spark) and another one by using Spark RDD/Dataframe so that enrichment of data happens in parallel. However, output of both sets of code should be the same.
3. Include unit tests with your code.
4. Package the code in a maven or gradle project.  
