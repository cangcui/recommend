package com.tencent.ieg.techcenter.recommend

import java.io.IOException

import com.tencent.ieg.techcenter.common.CommonUtility
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.reflect.ClassTag

/**
  * Created by lambdachen on 2016/7/21.
  */
class SpreadingModel[T: ClassTag](
     val lambda: Double,
     val weightMat: RDD[(T, T, Double)])
  extends Saveable with Serializable with Logging{

  protected override val formatVersion: String = "0.1"

  /**
    * save the model
    *
    * @param sc
    * @param path
    */
  override def save(sc: SparkContext, path: String): Unit ={
    SpreadingModel.SaveLoadV0_1.save(this, path)
  }

  /**
    *
    * @param userCollectedItems, target user's collected items
    * @param number, number of items(unfiltered) to recommend to targe user
    * @return
    */
  def recommendWithoutFiltering(userCollectedItems: Array[T],
                                number: Int) = {
    require(number > 0)
    SpreadingModel.recommendForUser(
      userCollectedItems,
      this.weightMat,
      number
    )
  }
}

/**
  * companion object of SpreadingModel
  * support the save()、load()、recommend() interfaces for class SpreadingModel
  *
  */
object SpreadingModel extends ExtLoader[SpreadingModel[_]]{

  private def schemaColumnNames = Seq("itemAlpha", "itemBeta", "heatWeight")

  private def recommendForUser[T: ClassTag](
      userCollectedItems: Array[T],
      weightMatrix: RDD[(T, T, Double)],
      number: Int): Array[(T, Double)] = {
    val aggregateByFirstItem = weightMatrix.map{
      case (item1, item2, w) =>
        (item1, Seq((item2, w)))
    }.reduceByKey(_ ++ _).cache()
    val collectedItemsSet = userCollectedItems.toSet

    val itemWithScores = aggregateByFirstItem.map{
      case (item1, itemsRow) =>
        (item1, itemsRow.filter(t => collectedItemsSet.contains(t._1)).map(_._2).sum)
    }.sortBy(_._2, ascending = false).take(number)
    aggregateByFirstItem.unpersist()

    itemWithScores
  }

  def load(sc: SparkContext, path: String) = {
    val (loadedClassName, formatVersion, metadata) = loadMetadata(sc, path)
    val classNameV0_1 = SaveLoadV0_1.thisClassName
    (loadedClassName, formatVersion) match{
      case (className, "0.1") if className == classNameV0_1 =>
        SaveLoadV0_1.load(sc, path)
      case _ =>
        throw new IOException("SpreadingModel.load did not recognize model with" +
          s"(class: $loadedClassName, version: $formatVersion). Supported:\n" +
          s"  ($classNameV0_1, 0.1)")
    }
  }

  /**
    * save and load object
    */
  object SaveLoadV0_1{
    private val thisFormatVersion = "0.1"
    val thisClassName = "SpreadingModel"

    def save[T: ClassTag](model: SpreadingModel[T], path: String): Unit ={
      val conf = new Configuration()
      val sc = model.weightMat.sparkContext
      val sqlContext = new SQLContext(sc)
      val metaData = compact(render(
        ("class" -> thisClassName) ~
          ("version" -> thisFormatVersion) ~
          ("lambda" -> model.lambda)
      ))
      CommonUtility.deletePath(metadataPath(path), conf)
      sc.parallelize(Seq(metaData), 1).saveAsTextFile(metadataPath(path))

      val weightRow = model.weightMat.map{
        case (item1, item2, w) => Row(item1, item2, w)
      }
      CommonUtility.deletePath(weightPath(path), conf)
      val schema = buildModelDataSchema(schemaColumnNames, weightRow.take(1)(0))
      sqlContext.createDataFrame(weightRow, schema).write.parquet(weightPath(path))
    }

    private def buildModelDataSchema(columnsNames: Seq[String], row: Row): StructType = {
      assert(columnsNames.length == 3)
      assert(row.length == 3)
      val schema = StructType(Seq(
        StructField(columnsNames(0), CommonUtility.constructFieldDataType(row(0))),
        StructField(columnsNames(1), CommonUtility.constructFieldDataType(row(1))),
        StructField(columnsNames(2), CommonUtility.constructFieldDataType(row(2)))
      ))
      schema
    }

    def load(sc: SparkContext, path: String) = {
      val sqlContext = new SQLContext(sc)
      val weightDataFrame = sqlContext.read.parquet(weightPath(path))
      val oneSrcVal = weightDataFrame.take(1)(0)(0)
      loadModel(sc, path, weightDataFrame, oneSrcVal)
    }

    private def loadModel[T: ClassTag](sc: SparkContext,
                                       path: String,
                                       weightDataFrame: DataFrame,
                                       srcValue: T): SpreadingModel[T] = {
      implicit val formats = DefaultFormats
      val (className, version, metadata) = loadMetadata(sc, path)
      assert(className == this.thisClassName)
      assert(version == this.thisFormatVersion)
      val lambda = (metadata \ "lambda").extract[Double]
      val weights = weightDataFrame.rdd.map{
        case Row(item1, item2, weight) =>
          (item1.asInstanceOf[T], item2.asInstanceOf[T], weight.asInstanceOf[Double])
      }
      new SpreadingModel(lambda, weights)
    }

    private def weightPath(path: String): String = {
      new Path(dataPath(path), "weight").toUri.toString
    }
  }
}
