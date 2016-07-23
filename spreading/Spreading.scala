package com.tencent.ieg.techcenter.recommend

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by lambdachen on 2016/7/21.
  *
  * The paper "Solving the apparent diversity-accuracy
  * dilemma of recommender systems" contains the detailed
  * information of this model.
  */

/**
  * represent the edge of (user, item) in bipartite graph
  *
  * @param user
  * @param item
  * @tparam T
  */
case class BipEdge[T](val user: T, val item: T)

/**
  * represents the element in Weight Matrix
  *
  * @param src1
  * @param src2
  * @param weight
  * @tparam T
  */
case class Weight[T](val src1: T, val src2: T, weight: Double)

class Spreading(private var lambda: Double) extends Serializable with Logging{

  def this() = this(0.5)

  def setLambda(lambda: Double): Unit ={
    this.lambda = lambda
  }

  /**
    * build the inverted index, the double value is the weight
    * between two items before normalization
    *
    * @param edges
    * @tparam T
    * @return
    */
  private[this] def buildInvertedIndex[T: ClassTag](edges: RDD[BipEdge[T]]): RDD[((T, T), Double)] = {
    val userItems = edges.map(b => (b.user, Array(b.item))).reduceByKey(_ ++ _).cache()
    val itemPairs = userItems.map{
      case (u, items) =>
        val ips = for{i <- items; j <- items if (i != j)} yield (i, j, 1.0 / items.length)
        ips
    }.flatMap(list => list).map{case (i1, i2, v) => ((i1, i2), v)}.reduceByKey(_ + _)
    userItems.unpersist()
    itemPairs
  }

  /**
    * compute the Weight Matrix
    *
    * @param edges
    * @tparam T
    * @return
    */
  def run[T: ClassTag](edges: RDD[BipEdge[T]]): SpreadingModel[T] = {
    val itemDegrees = edges.map(be => (be.item, 1)).reduceByKey(_ + _).cache()
    val itemPairs = buildInvertedIndex(edges).cache()

    val W = itemPairs.map{case ((item1, item2), v) => (item1, (item1, item2, v))}
      .join(itemDegrees)
      .map{case (item1Dup, ((item1, item2, v), ka)) => (item2, (item1, item2, v, ka))}
      .join(itemDegrees)
      .map{
        case (item2Dup, ((item1, item2, v, ka), kb)) =>
          (item1, item2, v/(math.pow(ka, 1 - this.lambda) * math.pow(kb, this.lambda)))
      }
    itemDegrees.unpersist()
    itemPairs.unpersist()
    new SpreadingModel[T](this.lambda, W)
  }
}

/**
  * companion object of class Spreading
  */
object Spreading{
  def train[T: ClassTag](edges: RDD[BipEdge[T]], lambda: Double) = {
    val sp = new Spreading(lambda)
    sp.run(edges)
  }
}


