package learning.memcached

import scala.collection.mutable.MutableList
import net.spy.memcached.internal.OperationFuture
import java.util.concurrent.TimeUnit
import scala.collection.mutable.HashMap
import java.util.concurrent.Future

class SGDPiecewiseMemClient extends SGDWeightMemClient {
  // Data structures to keep all async get requests
  private val getRequestsFutures = new HashMap[String, Future[Object]]
  private val getRequestsDone = new HashMap[String, Object]

  private val setRequestsFutures = new HashMap[String, Future[java.lang.Boolean]]

  ////////////////// Private methods /////////////////////////

  /*
   * Spymemcached seems to overflow memcached servers' queues if more than 100K
   * requests per thread (and 64 threads) are sent without waiting for their
   * results to come back. That's why we need to make sure that we always send
   * at most 100K requests to memcached.
   */
  private def avoidMemcachedFlood() : Unit = {
    if (getRequestsFutures.size > 1000) {
      val keys = getRequestsFutures.keySet
      for (key <- keys) {
        finalizeGetRequest(key)
      }
      assert(getRequestsFutures.size == 0)
    }
    if (setRequestsFutures.size > 5000) {
      val keys = setRequestsFutures.keySet
      for (key <- keys) {
        finalizeSetRequest(key)
      }
      assert(setRequestsFutures.size == 0)
    }
  }

  private def insertGetRequest(key : String) : Unit = {
    if (!getRequestsFutures.contains(key) && !getRequestsDone.contains(key)) {
      val f = hashtableRef.asyncGet(key)
      getRequestsFutures += ((key, f))
    }
    avoidMemcachedFlood()
  }
  private def finalizeGetRequest(key : String) : Object = {
    if (getRequestsDone.contains(key)) {
      return getRequestsDone.get(key).get
    }
    val f = getRequestsFutures.get(key).get
    var v : Object = null
    try {
      // https://code.google.com/p/spymemcached/issues/detail?id=136
      while (!f.isDone()) {
        Thread.sleep(5)
      }
      v = f.get(5, TimeUnit.SECONDS)
    } catch {
      case e : Exception => {
        f.cancel(false)
        System.err.println("MEMCACHED ERROR GET BULK!!! size requests futures = " +
            getRequestsFutures.size + " reqs done = " + getRequestsDone.size + " " + e.getMessage())
        getRequestsDone += ((key, null))
        getRequestsFutures -= key
        Thread.sleep(50)
      }
    }
    getRequestsDone += ((key, v))
    getRequestsFutures -= key
    v
  }



  private def insertSetRequest(key : String, v : Double) : Unit = {
    if (!setRequestsFutures.contains(key)) {
      val f = hashtableRef.set(key, 0, v)
      setRequestsFutures += ((key, f))
    }
    avoidMemcachedFlood()
  }
  private def finalizeSetRequest(key : String) = {
    val f = setRequestsFutures.get(key).get
    var v : Object = null
    try {
      // https://code.google.com/p/spymemcached/issues/detail?id=136
      while (!f.isDone()) {
        Thread.sleep(5)
      }
      v = f.get(5, TimeUnit.SECONDS)
    } catch {
      case e : Exception => {
        f.cancel(false)
        System.err.println("MEMCACHED ERROR SET BULK!!! size requests futures = " +
            setRequestsFutures.size + e.getMessage())
        setRequestsFutures -= key
        Thread.sleep(50)
      }
    }
    setRequestsFutures -= key
    v
  }

  ///////////// Public methods //////////////////////////////
  def sendRhoGetRequest(m : String, e : Int) : Unit = {
    val keyMaster = getRhoKey(m, e, -1)
    insertGetRequest(keyMaster)
  }

  def finalizeRhoGetRequest(m : String, e : Int) : Object = {
    val keyMaster = getRhoKey(m, e, -1)
    val resultMaster = finalizeGetRequest(keyMaster)
    resultMaster
  }
  def sendLambdaGetRequest(x1 : Int, x2 : Int) : Unit = {
    val keyMaster = getLambdaKey(x1, x2, -1)
    insertGetRequest(keyMaster)
  }

  def finalizeLambdaGetRequest(x1 : Int, x2 : Int) : Object = {
    val keyMaster = getLambdaKey(x1, x2, -1)
    val resultMaster = finalizeGetRequest(keyMaster)
    resultMaster
  }


  def sendRhoGetRequest(k : String) : Unit = {
    val keyMaster = "-1R\t" + k
    insertGetRequest(keyMaster)
  }
  def finalizeRhoGetRequest(k : String) : Double = {
    val keyMaster = "-1R\t" + k
    val resultMaster = finalizeGetRequest(keyMaster)
    if (resultMaster == null) {
      return 0
    }
    resultMaster.toString().toDouble
  }
  def sendLambdaGetRequest(k : Long) : Unit = {
    val keyMaster = "-1L\t" + k
    insertGetRequest(keyMaster)
  }
  def finalizeLambdaGetRequest(k : Long) : Double = {
    val keyMaster = "-1L\t" + k
    val resultMaster = finalizeGetRequest(keyMaster)
    if (resultMaster == null) {
      return 0
    }
    resultMaster.toString().toDouble
  }


  def sendRhoSetRequest(k : String, v : Double) : Unit = {
    val keyMaster = "-1R\t" + k
    insertSetRequest(keyMaster, v)
  }
  def sendLambdaSetRequest(k : Long, v : Double) : Unit = {
    val keyMaster = "-1L\t" + k
    insertSetRequest(keyMaster, v)
  }

  def finalizeAllGetAndSetRequests() : Unit = {
    for (key <- getRequestsFutures.keySet) {
      finalizeGetRequest(key)
    }
    for (key <- setRequestsFutures.keySet) {
      finalizeSetRequest(key)
    }
    assert(getRequestsFutures.size == 0)
    assert(setRequestsFutures.size == 0)
    getRequestsDone.clear
  }
}