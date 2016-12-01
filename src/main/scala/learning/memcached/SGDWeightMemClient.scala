package learning.memcached

import java.util.concurrent.TimeUnit
import java.util.concurrent.Future

import gnu.trove.map.hash.THashMap
import gnu.trove.set.hash.TLongHashSet

/*
 * Extension of memcached used to store the weights from an SGD algorithm.
 *
 * WorkerId should be -1 if no paralellism is involved.
 */
class SGDWeightMemClient extends MemClient {
  ///////////////////// API for mentionEntitiesFreq index //////////////
  def addMentionIndexEntry(mention : String, entities : Array[Int]) : Boolean = {
    set(getRhoKey(mention, 0, -2), entities.mkString(" "))
  }
  def getMentionIndexEntry(mentions : Array[String]) : THashMap[String, Array[Int]] = {
    val futures = new THashMap[String, Future[Object]]
    for (mention <- mentions) {
      val key = getRhoKey(mention, 0, -2)
      val f = hashtableRef.asyncGet(key)
      futures.put(key, f)
    }
    val rez = new THashMap[String, Array[Int]]
    for (mention <- mentions) {
      val key = getRhoKey(mention, 0, -2)
      val f = futures.get(key)
      var v : Object = null
      try {
        v = f.get(60, TimeUnit.SECONDS)
      } catch {
        case e : Exception => {
          f.cancel(false)
          System.err.println("MEMCACHED ERROR GET BULK IN INDEX !!! key = " + key)
        }
      }
      var toAdd = Array[Int]()
      if (v != null) toAdd = v.toString.split(" ").map(_.toInt).toArray
      rez.put(mention, toAdd)
    }
    rez
  }

  def addEntitiesIndexEntry(e : Int, entities : Iterable[Int]) : Boolean = {
    set(getLambdaKey(e, 0, -2), entities.mkString(" "))
  }
  def getEntitiesIndexEntry(ents : Array[Int], res : TLongHashSet) = {
    val futures = new THashMap[String, Future[Object]]
    for (e <- ents) {
      val key = getLambdaKey(e, 0, -2)
      val f = hashtableRef.asyncGet(key)
      futures.put(key, f)
    }
    for (e <- ents) {
      val key = getLambdaKey(e, 0, -2)
      val f = futures.get(key)
      var v : Object = null
      try {
        v = f.get(60, TimeUnit.SECONDS)
      } catch {
        case e : Exception => {
          f.cancel(false)
          System.err.println("MEMCACHED ERROR GET BULK IN INDEX !!! key = " + key)
        }
      }
      var toAdd = Array[Int]()
      if (v != null) toAdd = v.toString.split(" ").map(_.toInt).toArray
      for (e_prim <- toAdd) {
        res.add(getPartialLambdaKey(e, e_prim))
      }
    }
  }



  ///////////////////// API for Rhos //////////////
  def getPartialRhoKey(mention : String, entity : Int) : String = {
    val key = "" + entity + "\t" + mention.replace(' ', '_')
    if (key.getBytes("UTF-8").length < 240) {
      return key
    }
    System.err.println("Key too long : " + key)
    if (key.length() == key.getBytes("UTF-8").length) {
      return key.substring(0, 239)
    }
    key.substring(0, 30)
  }
  def getRhoKey(mention : String, entity : Int, workerID : Long) : String = {
    "" + workerID + "R\t" + getPartialRhoKey(mention, entity)
  }

  def addRho(mention : String, entity : Int, v : Double) : Boolean = {
    set(getRhoKey(mention, entity, -1), v)
  }

  // Set rho for a worker only if it was added
  def setWithRhoKey(k : String, v : Double, workerID : Long) : Boolean = {
    set("" + workerID + "R\t" + k, v)
  }

  // Returns true in case of success of get.
  def getWithRhoKey(k : String, workerID : Long) : (Double, Boolean) = {
    val o = get("" + workerID + "R\t" + k)
    if (o._1 == null) (0, o._2)
    else (o._1.toString.toDouble, o._2)
  }

  ///////////////////// API for Lambdas //////////////
  def getPartialLambdaKey(x1 : Int, x2 : Int) : Long = {
    getIntPairsKey(x1, x2)
  }
  def getLambdaKey(x1 : Int, x2 : Int, workerID : Long) : String = {
    "" + workerID + "L\t" + getIntPairsKey(x1, x2)
  }

  def addLambda(x1 : Int, x2 : Int, v : Double) : Boolean = {
    set(getLambdaKey(x1, x2, -1), v)
  }
  def setWithLambdaKey(k : Long, v : Double, workerID : Long) : Boolean = {
    set("" + workerID + "L\t" + k, v)
  }
  // Returns true in case of success of get.
  def getWithLambdaKey(k : Long, workerID : Long) : (Double, Boolean) = {
    val o = get("" + workerID + "L\t" + k)
    if (o._1 == null) (0, o._2)
    else (o._1.toString.toDouble, o._2)
  }

  ////////////////////// Get worker ids ///////////////////////////////////
  def generateWorkerID : Long = {
    val key = "workerID"
    add(key, 0)
    hashtableRef.incr(key, 1)
  }


}