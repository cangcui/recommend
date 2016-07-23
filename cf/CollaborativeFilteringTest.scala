package com.tencent.ieg.techcenter.tests

import com.tencent.ieg.techcenter.recommend._
import org.apache.spark.{SparkConf, SparkContext}
import scala.reflect.runtime.universe._

/**
  * for test
  */
object CollaborativeFilteringTest{

  def paramInfo[T](x: T)(implicit tag: TypeTag[T]) = {
    val targs = tag.tpe match { case TypeRef(_, _, args) => args }
    targs
  }

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    val testRDD = sc.makeRDD(Seq(
      Preference(1, 2, 10.0), Preference(1, 3, 30.0),
      Preference(1, 2, 1.0), Preference(1, 3, 3.0),
      Preference(2, 2, 2.0), Preference(2, 3, 4.0),
      Preference(3, 2, 3.0), Preference(3, 3, 5.0)
    ))
    val cf1 = new CollaborativeFiltering()
    cf1.setDistanceType(DistanceType.AdjustedCosine)
    val cf_model = CollaborativeFiltering.train(testRDD, 1, CFType.UserCF, DistanceType.Cosine)
    val basePath = args(0)
    cf_model.save(sc, basePath)

    val model = CollaborativeFilteringModel.load(sc, basePath)
    println(model.neighbourNumber)
    model.neighboursInformation.collect().foreach{
      case (src, dsts, sims) =>
        println(src)
        println(dsts.toSeq)
        println(sims.toSeq)
    }
    println(model.allNeighbours(2).toSeq)
    println(model.recommendNeighbours(2, 3).toSeq)
  }
}
