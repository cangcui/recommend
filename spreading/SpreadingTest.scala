package com.tencent.ieg.techcenter.tests

import com.tencent.ieg.techcenter.recommend._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * test object
  */
object SpreadingTest{
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName(getClass.getName)
    val sc = new SparkContext(conf)
    val edgesRDD = sc.makeRDD(Seq(
      BipEdge(1, 1), BipEdge(1, 4),
      BipEdge(2, 1), BipEdge(2, 2),
      BipEdge(2, 3), BipEdge(2, 4),
      BipEdge(3, 1), BipEdge(3, 3),
      BipEdge(4, 3), BipEdge(4, 5)
    ))
    val model = Spreading.train(edgesRDD, 0.5)
    val path = args(0)
    model.save(sc, path)

    val cpyModel = SpreadingModel.load(sc, path)
    cpyModel.weightMat.collect().foreach(println)

    val rItems = cpyModel.recommendWithoutFiltering(Array(1,2,3,4), 3)
    rItems.foreach(println)
  }
}
