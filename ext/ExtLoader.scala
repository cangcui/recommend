package com.tencent.ieg.techcenter.recommend

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.Saveable
import org.json4s.{DefaultFormats, JValue}

/**
  * Created by lambdachen on 2016/7/16.
  */

private[ieg] trait ExtLoader[M <: Saveable] {

  def dataPath(path: String) = new Path(path, "data").toUri.toString

  def metadataPath(path: String) = new Path(path, "metadata").toUri.toString

  def loadMetadata(sc: SparkContext, path: String): (String, String, JValue) = {
    implicit val formats = DefaultFormats
    val metadata = parse(sc.textFile(metadataPath(path)).first())
    val clazz = (metadata \ "class").extract[String]
    val version = (metadata \ "version").extract[String]
    (clazz, version, metadata)
  }

  def load(sc: SparkContext, path: String): M
}
