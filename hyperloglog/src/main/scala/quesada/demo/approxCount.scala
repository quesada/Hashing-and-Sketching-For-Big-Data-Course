package quesada.demo

/**
 * Created by quesada on 19/10/2015.
 */

trait ApproximateCounter {
  def add(id: Long): Unit
  def distinctCount(): Double
}



// naive implementation
class NaiveCounter extends ApproximateCounter{

  val ids = scala.collection.mutable.HashSet[Long]()

  override def add(id: Long): Unit = {
    ids.add(id)
  }

  override def distinctCount(): Double = {
    ids.size
  }

}

// Hyperloglog implementation

import com.carrotsearch.hppc.BitMixer
import com.clearspring.analytics.stream.cardinality.{HyperLogLog => HLL}
//import com.sun.java.util.jar.pack.Histogram.BitMetric
import org.elasticsearch.common.util.BigArrays
import org.elasticsearch.search.aggregations.metrics.cardinality.HyperLogLogPlusPlus

class streamCounter extends ApproximateCounter{

  val hLL = new HLL(16)// magic number. some sd of the precision hLL.distinctCount

  override def add(id: Long): Unit = {
    hLL.offer(id)
  }

  override def distinctCount(): Double = {
    hLL.cardinality()
  }

}


// Hyperloglog PLUS implementation
// TODO read up on the differences

import com.clearspring.analytics.stream.cardinality.{HyperLogLogPlus => HLLplus}
class streamCounterPlus extends ApproximateCounter{

  val hLLplus = new HLLplus(16)// magic number. exponent for 2. This is bits representing the hash

  override def add(id: Long): Unit = {
    hLLplus.offer(id)
  }

  override def distinctCount(): Double = {
    hLLplus.cardinality()
  }

}

//
//import quesada.demo.YonikMurmurHash3
//class streamCounterPlus extends ApproximateCounter{
//
//  override def add(id: Long): Unit = {
//    val hash = new ()
//    MurMur.offer(id)
//  }
//
//  override def distinctCount(): Double = {
//    MurMur.cardinality()
//  }
//
//}

// Hyperloglogplusplus elasticsearch implementations
// ---------------------------------------------------------
// They differ from previous implementations in that you have to create a hash key before adding it to the hash.
// There are two hashing functions here, bitmixer and murmur

// TODO read up on the differences
// Bitmixer
class streamCounterPlusPlus extends ApproximateCounter{

  val hLLplusplus = new HyperLogLogPlusPlus(16, BigArrays.NON_RECYCLING_INSTANCE, 1)// magic number, and constant.

  override def add(id: Long): Unit = {
    val hash = BitMixer.mix64(id)
    hLLplusplus.collect(0, hash)
  }

  override def distinctCount(): Double = {
    hLLplusplus.cardinality(0)
  }

}

// MurMur hashing
class streamCounterPluslPlusMurMur extends ApproximateCounter{

  val hLLplusplusMurMur = new HyperLogLogPlusPlus(16, BigArrays.NON_RECYCLING_INSTANCE, 1)// magic number, and constant.

  override def add(id: Long): Unit = {
    val hash = Murmur3Hasher.hash(id)
    hLLplusplusMurMur.collect(0, hash) // collect is already part of elasticsearch
  }

  override def distinctCount(): Double = {
    hLLplusplusMurMur.cardinality(0)
  }

}



//See how many items we lose by using the approximations

object approxCount {

  def main(args: Array[String]) {

    // init counters
    val nai = new NaiveCounter()
    val hLL = new streamCounter()
    val hLLplus = new streamCounterPlus()
    val hLLplusplus = new streamCounterPlusPlus()
    val hLLplusplusMurMur = new streamCounterPluslPlusMurMur()

    for (i <- 0 until 100000) {
      nai.add(i)
      hLL.add(i)
      hLLplus.add(i)
      hLLplusplus.add(i)
      hLLplusplusMurMur.add(i)

      // Printing the estimated counts for each different hyperLogLog method
      // Remember we only care about the total number of unique elemens
      println(nai.distinctCount(), hLL.distinctCount(), hLLplus.distinctCount(), hLLplusplus.distinctCount(), hLLplusplusMurMur.distinctCount())

    }


  }
}