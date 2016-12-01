package learning.memcached

import net.spy.memcached.AddrUtil
import net.spy.memcached.MemcachedClient
import java.util.concurrent.TimeUnit
import net.spy.memcached.BinaryConnectionFactory
import net.spy.memcached.ConnectionFactoryBuilder
import net.spy.memcached.FailureMode


/*
 * Memcached client that has set/get/contains for various objects.
 *
 *  @param atMaster - if this client is on the master node or on a worker (connect to different machines)
 */
class MemClient {

  protected val builder = new ConnectionFactoryBuilder()
  builder.setShouldOptimize(false);
  builder.setFailureMode(FailureMode.Retry);
  builder.setOpQueueMaxBlockTime(120000);
  builder.setOpTimeout(120000);


  protected val hashtableRef = new MemcachedClient(
      // new BinaryConnectionFactory(),
      builder.build(),
      // Use different machines than Spark to avoid GC bad effects on memcached (bug #136 in spymemcached)
      AddrUtil.getAddresses("dco-node035.dco.ethz.ch:11211 dco-node036.dco.ethz.ch:11211"));
  /*
      AddrUtil.getAddresses("dco-node040.dco.ethz.ch:11211 dco-node035.dco.ethz.ch:11211 " +
          "dco-node033.dco.ethz.ch:11211 dco-node034.dco.ethz.ch:11211 dco-node036.dco.ethz.ch:11211" +
          " dco-node037.dco.ethz.ch:11211 dco-node038.dco.ethz.ch:11211 dco-node039.dco.ethz.ch:11211")
  */

  protected def add(key : String, value : Any) : Boolean = {
    val f = hashtableRef.add(key, 0, value)
    try {
      val v = f.get(60, TimeUnit.SECONDS)
      return v
    } catch {
      case e : Exception => {
        f.cancel(false)
        System.err.println("MEMCACHED ERROR !!!")
        return false
      }
    }
  }
  protected def set(key : String, value : Any) : Boolean = {
    val f = hashtableRef.set(key, 0, value)
    try {
      val v = f.get(60, TimeUnit.SECONDS)
      return v
    } catch {
      case e : Exception => {
        f.cancel(false)
        System.err.println("MEMCACHED ERROR !!!")
        return false
      }
    }
  }

  protected def get(key : String) : (Object, Boolean) = {
    val f = hashtableRef.asyncGet(key)
    var value : Object = null
    try {
      value = f.get(60, TimeUnit.SECONDS)
      return (value, true)
    } catch {
      case e : Exception => {
        f.cancel(false)
        System.err.println("MEMCACHED ERROR !!!")
        return (null, false)
      }
    }
    (null, false)
  }
  protected def delete(key : String) : Boolean = {
    val f = hashtableRef.delete(key)
    try {
      val v = f.get(60, TimeUnit.SECONDS)
      return v
    } catch {
      case e : Exception => {
        f.cancel(false)
        System.err.println("MEMCACHED ERROR !!!")
        return false
      }
    }
  }

  def flushAll : Boolean = hashtableRef.flush().get(15, TimeUnit.SECONDS)
  def shutdown = hashtableRef.shutdown()
  //////////////////////// API for pairs of integers ////////////////////////
  def getIntPairsKey(x1 : Int, x2 : Int) : Long = {
    return ((Math.min(x1,x2).toLong  << 31) + Math.max(x1,x2).toLong)
  }
  def unfoldIntPairsKey(k : Long) : (Int, Int) = {
    return ((k >> 31).toInt, (k & ((1 << 31) - 1)).toInt)
  }

  ///////////////////// API for frequency of pairs of integers //////////////
  def setF(x1 : Int, x2 : Int, value : Int) : Boolean = {
    set("F\t" + getIntPairsKey(x1, x2), value)
  }
  // Returns true in case of success of get.
  def getF(x1 : Int, x2 : Int) : (Int, Boolean) = {
    val o = get("F\t" + getIntPairsKey(x1, x2))
    if (o._1 == null) (-1, o._2)
    else (o._1.toString.toInt, o._2)
  }
  def containsF(x1 : Int, x2 : Int) : Boolean = {
    val o = get("F\t" + getIntPairsKey(x1, x2))
    o._1 != null
  }
}