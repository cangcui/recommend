package com.tencent.ieg.techcenter.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, MapType, _}
import org.slf4j.Logger

import scala.collection.mutable
import scala.reflect.io.{File, Path}


/**
  * Created by lambdachen on 2016/7/18.
  */
object CommonUtility {

  var CURRENT_COMMON_LOGGER: Logger = null

  /**
    * delete the file or directory
    * */
  def deletePath(givenPath: String, conf: Configuration): Unit ={
    if(givenPath.startsWith("hdfs")){
      val fs = hadoop.fs.FileSystem.get(conf)
      val path = new hadoop.fs.Path(givenPath)
      if(fs.exists(path)){
        fs.delete(path, true)
      }
    }else{
      val outFile = File(givenPath)
      val outPath = Path(givenPath)
      if (outFile.exists) {
        if (outFile.isFile) {
          outFile.delete()
        }else if(outFile.isDirectory) {
          outPath.deleteRecursively()
          outFile.delete()
        }
      }
    }
  }

  /**
    * consctruct the DataType over x
    *
    * @param x
    * @return corresponding DataType of x, e.g x:Map[String, Int], then return MapType[StringType, IntegerType]
    * */
  def constructFieldDataType(x: Any): DataType = {
    x.getClass().toString match{
      case sInt: String if sInt.contains("Int") => IntegerType
      case sDouble: String if sDouble.contains("Double") => DoubleType
      case sStr: String if sStr.contains("String") => StringType
      case lStr: String if lStr.contains("Long") => LongType
      case listStr: String if listStr.contains("List") =>
        val oneElement = x.asInstanceOf[List[Any]](0)
        ArrayType(constructFieldDataType(oneElement))
      case waStr: String if waStr.contains("WrappedArray") =>
        val oneElement = x.asInstanceOf[mutable.WrappedArray[Any]](0)
        ArrayType(constructFieldDataType(oneElement))
      case aStr: String if aStr.contains("Array") =>
        val oneElement = x.asInstanceOf[Array[Any]](0)
        ArrayType(constructFieldDataType(oneElement))
      case mStr: String if mStr.contains("Map") =>
        val oneK = x.asInstanceOf[Map[Any, Any]].keys.head
        val oneV = x.asInstanceOf[Map[Any, Any]].values.head
        MapType(constructFieldDataType(oneK), constructFieldDataType(oneV), true)
      case _ =>
        CURRENT_COMMON_LOGGER.error("undefined DataType in fcuntion " +
          "CommonUtility.constructFieldDataType(), x: " + x + ", getClass: " + x.getClass)
        StringType
    }
  }
}