package com.tencent.ieg.techcenter.recommend

import com.tencent.ieg.techcenter.common.CommonUtility
import org.apache.cassandra.io.compress.ICompressor.WrappedArray
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Created by lambdachen on 16/7/17.
  */

/**
  * user represents the user, item represents the item, rating represents the score
  *
  * @param user
  * @param item
  * @param rating
  * @tparam T
  */
case class Preference[T](user: T, item: T, rating: Double)

/**
  * represents the similarity between two users or items
  *
  * @param src1
  * @param src2
  * @param similarity
  * @tparam T
  */
case class Similarity[T](src1: T, src2: T, similarity: Double)

/**
  * represents a neighbour of src, it contains the dst and similarity value
  *
  * @param dst
  * @param similarity
  * @tparam T
  */
case class Neighbour[T](val dst: T, val similarity: Double)
  extends Ordered[Neighbour[T]] with Serializable {
  def compare(that: Neighbour[T]): Int = that.similarity.compareTo(similarity)
}

/**
  * Distance Type
  */
object DistanceType extends Enumeration{
  type DistanceType = Value
  val Cosine, AdjustedCosine = Value
}
import DistanceType._

/**
  * Collaborative Filtering Type
  */
object CFType extends Enumeration{
  type CFType = Value
  val ItemCF, UserCF = Value
}
import CFType._

/**
  *
  * @param neighbourNumber
  */
class CollaborativeFiltering(private var neighbourNumber: Int,
                             private var cfType: CFType,
                             private var distanceType: DistanceType) extends Serializable with Logging{
  def this() = this(10, CFType.ItemCF, DistanceType.Cosine)

  def setNeighbourNumber(num: Int): this.type = {
    this.neighbourNumber = num
    this
  }

  def setDistanceType(dtype: DistanceType): this.type = {
    this.distanceType = dtype
    this
  }

  def setCFType(cfType: CFType): this.type = {
    this.cfType = cfType
    this
  }

  /**
    * The public model run interface
    *
    * @param prefs
    * @tparam T
    * @return
    */
  def run[T: ClassTag](prefs: RDD[Preference[T]]): CollaborativeFilteringModel[T] = {
    val distinctRDD = prefs.distinct()
    (this.cfType, this.distanceType) match{
      case (CFType.ItemCF, DistanceType.Cosine) =>
        runCFCosineBased(distinctRDD)
      case (CFType.ItemCF, DistanceType.AdjustedCosine) =>
        runItemCFAdjustedCosineBased(distinctRDD)
      case (CFType.UserCF, DistanceType.Cosine) =>
        runCFCosineBased(distinctRDD)
    }
  }

  /**
    * If the distnace type is consine type, then invoke this function
    *
    * @param prefs
    * @tparam T
    * @return
    */
  private[this] def runCFCosineBased[T: ClassTag](prefs: RDD[Preference[T]]): CollaborativeFilteringModel[T] = {
    val combinePrefs = this.cfType match{
      case CFType.ItemCF =>
        prefs.map(p => (p.item, ArrayBuffer((p.user, p.rating))))
          .reduceByKey(_ ++ _).cache()
      case CFType.UserCF =>
        prefs.map(p => (p.user, ArrayBuffer((p.item, p.rating))))
          .reduceByKey(_ ++ _).cache()
    }
    val cpyCombinePrefs = combinePrefs

    val cartesianPres = combinePrefs.cartesian(cpyCombinePrefs).filter{
      case (v1, v2) => v1._1 != v2._1
    }
    val neighbours = cartesianPres.map{
      case (v1, v2) =>
        val sim = cosineBasedSimilarity(v1, v2)
        (sim.src1, Array(new Neighbour(sim.src2, sim.similarity)))
    }.reduceByKey(_ ++ _).map{
      case (src, arr) =>
        val takeNum = math.min(this.neighbourNumber, arr.size)
        (src, arr.sorted.take(takeNum))
    }

    val refactorNeighbours = neighbours.map{
      case (src, ns) =>
        (src, ns.map(n => n.dst), ns.map(n => n.similarity))
    }
    combinePrefs.unpersist()
    cpyCombinePrefs.unpersist()
    new CollaborativeFilteringModel(this.neighbourNumber, refactorNeighbours)
  }

  /**
    * If the distance type is adjusted cosine type and cf type is item-cf,
    * then invoke this function
    *
    * @param prefs
    * @tparam T
    * @return
    */
  private[this] def runItemCFAdjustedCosineBased[T: ClassTag](prefs: RDD[Preference[T]]): CollaborativeFilteringModel[T] = {
    val userMeanRating = prefs.map(p => (p.user, (1, p.rating)))
      .reduceByKey{case (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)}
      .map{case (src, (num, totalRatings)) => (src, totalRatings / num.toDouble)}

    val itemUsers = prefs.map(p => (p.user, (p.item, p.rating))).join(userMeanRating)
      .map{case (user, ((item, rat), userMean)) => (item, ArrayBuffer((user, rat, userMean)))}
      .reduceByKey(_ ++ _).cache()
    val cpyItemUsers = itemUsers
    val cartesianItemUsers = itemUsers.cartesian(cpyItemUsers).filter{
      case (v1, v2) => v1._1 != v2._1
    }

    val neighbours = cartesianItemUsers.map{
      case (v1, v2) =>
        val sim = adjustedCosineBasedSimilarity(v1, v2)
        (sim.src1, Array(new Neighbour(sim.src2, sim.similarity)))
    }.reduceByKey(_ ++ _).map{
      case (src, arr) =>
        val takeNum = math.min(this.neighbourNumber, arr.size)
        (src, arr.sorted.take(takeNum))
    }

    val refactorNeighbours = neighbours.map{
      case (src, ns) =>
        (src, ns.map(n => n.dst), ns.map(n => n.similarity))
    }
    itemUsers.unpersist()
    new CollaborativeFilteringModel[T](this.neighbourNumber, refactorNeighbours)
  }

  /**
    * compute the adjusted cosine distance between two vectors
    *
    * @param vectorA the first element is the user(or item), and the
    *               array is the rating of items(or the rating given by the users)
    * @param vectorB
    * @tparam T
    * @return
    */
  private[this] def adjustedCosineBasedSimilarity[T: ClassTag](
      vectorA: (T, ArrayBuffer[(T, Double, Double)]),
      vectorB: (T, ArrayBuffer[(T, Double, Double)])): Similarity[T] = {
    val aSet = vectorA._2.map(_._1).toSet
    val bSet = vectorB._2.map(_._1).toSet
    val interSet = aSet.intersect(bSet)
    if(interSet.isEmpty){
      Similarity(vectorA._1, vectorB._1, 0.0)
    }else{
      val interUserInA = vectorA._2.filter(p => interSet.contains(p._1))
      val interUserInB = vectorB._2.filter(p => interSet.contains(p._1))
      assert(interUserInA.length == interUserInB.length)
      val commonRats = interUserInA.sortBy(_._1.toString).zip(interUserInB.sortBy(_._1.toString))
      val squareErr1 = interUserInA.map{case (u, r, um) => (r - um) * (r - um)}.sum
      val squareErr2 = interUserInB.map{case (u, r, um) => (r - um) * (r - um)}.sum
      val covar = commonRats.map{
        case (t1, t2) =>
          (t1._2 - t1._3) * (t2._2 - t2._3)
      }.sum
      Similarity(vectorA._1, vectorB._1, covar / math.sqrt(squareErr1 * squareErr2))
    }
  }

  /**
    * compute the cosine distance between two vectors
    *
    * @param vectorA, the first element is the user(or item), and the
    *               array is the rating of items(or the rating given by the users)
    * @param vectorB
    * @tparam T
    * @return
    */
  private[this] def cosineBasedSimilarity[T: ClassTag](
      vectorA: (T, ArrayBuffer[(T, Double)]),
      vectorB: (T, ArrayBuffer[(T, Double)])): Similarity[T] = {
    val aSet = vectorA._2.map(_._1).toSet
    val bSet = vectorB._2.map(_._1).toSet
    val interSet = aSet.intersect(bSet)
    if(interSet.isEmpty){
      Similarity(vectorA._1, vectorB._1, 0.0)
    }else{
      val s = interSet.size.toDouble / math.sqrt(aSet.size.toDouble * bSet.size.toDouble)
      Similarity(vectorA._1, vectorB._1, s)
    }
  }

  private[this] def getDistanceFunction[T: ClassTag]() = {
    this.distanceType match {
      case DistanceType.Cosine =>
        cosineBasedSimilarity[T]_
    }
  }

}

/**
  * Companion object of CollaborativeFiltering
  *
  */
object CollaborativeFiltering{

  /**
    *
    * @param preferences
    * @param neighbourNumber
    * @param cfType
    * @param distanceType
    * @tparam T
    * @return
    */
  def train[T: ClassTag](
      preferences: RDD[Preference[T]],
      neighbourNumber: Int,
      cfType: CFType,
      distanceType: DistanceType): CollaborativeFilteringModel[T] = {
    val cf = new CollaborativeFiltering(neighbourNumber, cfType, distanceType)
    cf.run(preferences)
  }

}


