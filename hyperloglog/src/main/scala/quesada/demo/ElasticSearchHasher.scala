package quesada.demo

/**
 * Created by quesada on 20/10/2015.
 */

import com.carrotsearch.hppc.BitMixer

object ElasticSearchHasher {
 def hash(obj: Long): Long = {
    BitMixer.mix64(obj)
  }
}

