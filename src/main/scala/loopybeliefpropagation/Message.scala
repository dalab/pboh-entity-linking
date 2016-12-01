package loopybeliefpropagation

import md.Mention

class Message(val from : Mention, val to : Mention, val entity : Int) {
  override def equals(obj : Any) : Boolean = {
    if (!obj.isInstanceOf[Message]) {
      return false;
    }
    val msg = obj.asInstanceOf[Message]
    return from.equals(msg.from) && to.equals(msg.to) && entity == msg.entity
  }
  
  override def hashCode() : Int = {
    return from.hashCode() + 17 * to.hashCode + 97 * entity;
  }

}