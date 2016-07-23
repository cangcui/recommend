package com.tencent.ieg.techcenter.recommend

import java.io.IOException

import com.tencent.ieg.techcenter.common.CommonUtility
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{DataType, StringType, _}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.reflect.ClassTag
import scala.collection.mutable.WrappedArray


/**
  * Created by lambdachen on 16/7/17.
  */

/**
  * Model representation of CollaborativeFiltering
  * in Item-cf, the neighboursInformation stores the items' neighbours
  * in User-cf, the neighboursInformation stores the users' neighbours
  *
  * @param neighbourNumber
  * @param neighboursInformation
  * @tparam T
  */
class CollaborativeFilteringModel[T: ClassTag](
    val neighbourNumber: Int,
    val neighboursInformation: RDD[(T, Array[T], Array[Double])])
  extends Saveable with Serializable with Logging {

  require(neighbourNumber > 0)
  validateNeighbours("neighbours", neighboursInformation)

  protected override val formatVersion: String = "0.1"

  private def validateNeighbours(name: String,
                                 neighbours: RDD[(T, Array[T], Array[Double])]): Unit ={
    if(neighbours.partitioner.isEmpty){
      logWarning(s"$name factor does not have a partitioner. "
        + "Prediction on individual records could be slow.")
    }
    if(neighbours.getStorageLevel == StorageLevel.NONE){
      logWarning(s"$name factor is not cached. Prediction could be slow.")
    }
  }

  override def save(sc: SparkContext, path: String): Unit ={
    CollaborativeFilteringModel.SaveLoadV0_1.save(this, path)
  }

  def allNeighbours(src: T): Array[Neighbour[T]] = {
    recommendNeighbours(src, this.neighbourNumber)
  }

  def recommendNeighbours(src: T, recommendNumber: Int): Array[Neighbour[T]] = {
    CollaborativeFilteringModel.recommendNeigbhours(
      src,
      this.neighboursInformation,
      recommendNumber)
  }
}

/**
  * companion object of model CollaborativeFilteringModel
  */
object CollaborativeFilteringModel extends ExtLoader[CollaborativeFilteringModel[_]]{

  protected val schemaCloumnNames = Seq("src", "dsts", "sims")

  private def recommendNeigbhours[T: ClassTag](
       src: T,
       neighboursInformation: RDD[(T, Array[T], Array[Double])],
       recommendNumber: Int) = {
    val zipNeighboursAndSims = neighboursInformation.map{
      case (source, dsts, sims) =>
        (source, dsts.zip(sims).map{case (d, s) => new Neighbour[T](d, s)})
    }
    val ngbs = zipNeighboursAndSims.lookup(src)
    if(ngbs.nonEmpty){
      ngbs.head.take(recommendNumber)
    }else{
      Array.empty[Neighbour[T]]
    }
  }

  override def load(sc: SparkContext, path: String) = {
    val (loadedClassName, formatVersion, _) = loadMetadata(sc, path)
    val classNameV0_1 = SaveLoadV0_1.thisClassName
    (loadedClassName, formatVersion) match{
      case (className, "0.1") if className == classNameV0_1 =>
        SaveLoadV0_1.load(sc, path)
      case _ =>
        throw new IOException("MatrixFactorizationModel.load did not recognize model with" +
          s"(class: $loadedClassName, version: $formatVersion). Supported:\n" +
          s"  ($classNameV0_1, 0.1)")
    }
  }

  /**
    * SaveLoadV0_1 object
    */
  private[recommend]
  object SaveLoadV0_1{
    private val thisFormatVersion = "0.1"
    val thisClassName = "CollaborativeFilteringModel"

    /**
      * use this function to save model
      *
      * @param model
      * @param path
      */
    def save[T: ClassTag](model: CollaborativeFilteringModel[T], path: String): Unit ={
      val conf = new Configuration()
      val sc = model.neighboursInformation.sparkContext
      val sqlContext = new SQLContext(sc)
      val metadata = compact(render(
        ("class" -> thisClassName) ~
          ("version" -> thisFormatVersion) ~
          ("neighbourNumber" -> model.neighbourNumber)
      ))
      val neighbours = model.neighboursInformation
      CommonUtility.deletePath(metadataPath(path), conf)
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(metadataPath(path))

      val rows = neighbours.map{
        case (src, dsts, sims) => Row(src, dsts, sims)
      }
      val ngbSchema = buildModeldataSchema(schemaCloumnNames, rows.take(1)(0))
      CommonUtility.deletePath(neighboursPath(path), conf)
      sqlContext.createDataFrame(rows, ngbSchema).write.parquet(neighboursPath(path))
    }

    /**
      * use this function to load function
      *
      * @param sc
      * @param path
      * @return
      */
    def load(sc: SparkContext, path: String) = {
      val sqlContext = new SQLContext(sc)
      val neighboursDataFrame = sqlContext.read.parquet(neighboursPath(path))
      val oneSrcValue = neighboursDataFrame.take(1)(0)(0)
      loadModel(sc, neighboursDataFrame, path, oneSrcValue)
    }

    private def loadModel[T: ClassTag](sc: SparkContext,
                                       neighboursDataFrame: DataFrame,
                                       path: String,
                                       srcValue: T) = {
      implicit val formats = DefaultFormats
      val (className, formatVersion, metadata) = loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)

      val neighbourNumber = (metadata \ "neighbourNumber").extract[Int]
      val neighboursInformation = neighboursDataFrame.map{
        case Row(src, dsts, sims) =>
          val dstsArr = dsts.asInstanceOf[WrappedArray[T]].toArray
          val simsArr = sims.asInstanceOf[WrappedArray[Double]].toArray
          (src.asInstanceOf[T], dstsArr, simsArr)
      }
      new CollaborativeFilteringModel(neighbourNumber, neighboursInformation)
    }

    private def buildModeldataSchema(names: Seq[String], row: Row): StructType = {
      require(names.length >= 3)
      val srcType = CommonUtility.constructFieldDataType(row(0))
      val schema = StructType(Seq(
        StructField(names(0), srcType),
        StructField(names(1), ArrayType(srcType)),
        StructField(names(2), ArrayType(DoubleType))
      ))
      schema
    }

    private def neighboursPath(path: String): String = {
      new Path(dataPath(path), "neighbours").toUri.toString
    }
  }
}
