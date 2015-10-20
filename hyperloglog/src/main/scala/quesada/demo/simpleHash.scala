package quesada.demo

import scala.collection.mutable.ArrayBuffer

/**
 * Created by quesada on 20/10/2015.
* a hashset has no values
 *
 * do not implement the hash function (that generates the index)
 *
 */
class SimpleHashSet {

  // size the array that will host the hash's keys
  val m = 1024

  var table = Array.fill(m){new ArrayBuffer[Long]}
  // init the Array with actual Longs, otherwise it's only placeholders


  // hack: abs to avoid negative numbers
  // This implementation uses an operation that is very costly : modulo
  // there's a much better way to do it.

  // The problem is that the hash function we have generates 64 bit numbers
  // we don't want a 64 bit number, it's too big to be used as the index of
  // our array.

  // A solution is to use this modulo

  
  def add(element: Long): Unit = {

    val index = Math.abs(Murmur3Hasher.hash(element) % m)
    val list = table(index.toInt)
    if (!list.contains(element)) list += element
  }

  def contains(element: Long): Boolean = {
    val index = Math.abs(Murmur3Hasher.hash(element) % 1024)
    val list = table(index.toInt)
    list.contains(element)
  }

}

object TestHash {
  def main(args: Array[String]) {
    val a = 188 // 189 produced Exception in thread "main" java.lang.ArrayIndexOutOfBoundsException: -1002
    val c = 189 // Exception in thread "main" java.lang.NullPointerException
    val b = 4223 // larger than the modulo, to see if it break
    var shash = new SimpleHashSet
    shash.add(c.toInt)
    shash.add(b.toInt)
    println(shash.contains(a)) // false, correct
    println(shash.contains(b)) // index out of bounds -590

  }

}
