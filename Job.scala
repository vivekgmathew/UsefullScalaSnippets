package dc.basics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.log4j.Logger
import org.apache.log4j.Level


object Job extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder().
    appName("CapitalOne-COBOL").
    master("local").
    getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val cpudSchema = StructType (
     Array(
       StructField("CPUD_ORG", StringType),
       StructField("CPUD_POS", StringType),
       StructField("CPUD_ITEMS", ArrayType (
         StructType(Array(
           StructField("CPUD_ITEM_DESC", StringType),
           StructField("CPUD_QUANTITY", StringType))
         )
       ))
     )
  )

  val cpudData = Seq(
    Row("AA", "Walmart", List(Row("Pen", "2"), Row("Book", "3"))),
    Row("AA", "Target", List(Row("Bed", "1"), Row("Pillow", "2")))
  )

  val cpDf = spark.createDataFrame(spark.sparkContext.parallelize(cpudData), cpudSchema)
  cpDf.show()
  cpDf.printSchema()

  // Actual logic to explode and then flatten the array of struct values
  // You can use the same for your data.
  val explodedDf = cpDf.select($"CPUD_ORG", $"CPUD_POS", explode($"CPUD_ITEMS").as("CPUD_ITEMS"))
  explodedDf.show()

  case class ItemDetails(var item: String, var qty: String)
  case class Sku(var account: String, var items: Seq[ItemDetails])

  val gp =  explodedDf.groupBy($"CPUD_POS").agg(collect_list($"CPUD_ITEMS").as("CPUD_ITEMS"))
  gp.show()
  gp.printSchema()

  val mp = gp.rdd.map(it => it.getString(0) -> it.getList[Row](1)).collectAsMap()
  println(mp)

  import scala.collection.mutable.{Map => MutMap}
  import scala.collection.mutable.{Seq => MutSeq}

  val mtp = MutMap[String, Seq[ItemDetails]]()

  mp.keys.foreach(key => {
    var mts = MutSeq[ItemDetails]()
    for(i <- 0 to mp.get(key).get.size - 1) {
      var obj: ItemDetails = ItemDetails("", "")
      for(j <- 0 to mp.get(key).get.get(i).size - 1) {
        obj = j match {
          case 0 => obj.item = mp.get(key).get.get(i).get(j).toString; obj
          case 1 => obj.qty = mp.get(key).get.get(i).get(j).toString; obj
        }
      }
      mts :+= obj
    }
    mtp += (key -> mts)
  })


  println(mtp)

  var skuSeq = MutSeq[Sku]()
  mtp.keys.foreach(key => {
    skuSeq :+= Sku(key, mtp.get(key).get)
  })

  skuSeq.toDF().write.json("src/main/resources/sku")

}
