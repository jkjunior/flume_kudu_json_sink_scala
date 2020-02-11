import java.util.UUID

import com.google.common.hash.Hashing

import scala.collection.mutable.Set
import scala.util.Random
import scala.util.hashing.MurmurHash3

var set = Set[String]()
for (i <- 1 to 40000000) {
//  set += MurmurHash3.bytesHash(UUID.randomUUID().toString.getBytes)
//  set += UUID.randomUUID().toString
  set += Hashing.murmur3_128().hashBytes(UUID.randomUUID().toString.getBytes).toString
}
println(set.size)