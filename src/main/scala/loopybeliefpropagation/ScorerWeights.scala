package loopybeliefpropagation

class ScorerWeights(var a : Double, var f : Double, var g : Double, var h : Double, var s : Double, var b : Double) extends Serializable {
  
  def this() {
    this(0.5, 1.0, 1.0, 1.0, 0.0, 0.0)
  }
  
  
  //////// For scorer & context words ////////
  def this(aa : Int,a : Double, f : Double, g : Double, h : Double, s : Double, b : Double, delta_w_e : Double, delta_cocit : Double, cocit_e_e_param : Double) = {
    this(a,f,g,h,s,b)
    this.delta_w_e = delta_w_e
    this.xi = xi
    this.delta_cocit = delta_cocit
    this.cocit_e_e_param = cocit_e_e_param
  }

  var delta_w_e : Double = 0.0
  var xi : Double = 0.001
  
  var delta_cocit : Double = 0.0
  
  var cocit_e_e_param : Double = 0.1
  ////////////////////////////////////////////
  
  /*
   * Historical sum of squares of gradients used in AdaGrad.
   */
  var num_grads = 0
  
  var squares_grad_a : Double = 0
  var squares_grad_f : Double = 0
  var squares_grad_g : Double = 0
  var squares_grad_h : Double = 0
  var squares_grad_s : Double = 0
  var squares_grad_b : Double = 0

  var sum_grad_a : Double = 0
  var sum_grad_f : Double = 0
  var sum_grad_g : Double = 0
  var sum_grad_h : Double = 0
  var sum_grad_s : Double = 0
  var sum_grad_b : Double = 0
  
  
  def add(w : ScorerWeights) : ScorerWeights = {
    a += w.a
    f += w.f
    g += w.g
    h += w.h
    s += w.s
    b += w.b
    sum_grad_a += w.sum_grad_a
    sum_grad_f += w.sum_grad_f
    sum_grad_g += w.sum_grad_g
    sum_grad_h += w.sum_grad_h
    sum_grad_s += w.sum_grad_s
    sum_grad_b += w.sum_grad_b
    squares_grad_a += w.squares_grad_a
    squares_grad_f += w.squares_grad_f
    squares_grad_g += w.squares_grad_g
    squares_grad_h += w.squares_grad_h
    squares_grad_s += w.squares_grad_s
    squares_grad_b += w.squares_grad_b
    this
  }
  def multiply(ct : Double) : ScorerWeights = {
    sum_grad_a *= ct
    sum_grad_f *= ct
    sum_grad_g *= ct
    sum_grad_h *= ct
    sum_grad_s *= ct
    sum_grad_b *= ct
    squares_grad_a *= ct
    squares_grad_f *= ct
    squares_grad_g *= ct
    squares_grad_h *= ct
    squares_grad_s *= ct
    squares_grad_b *= ct
    a *= ct
    f *= ct
    g *= ct
    h *= ct
    s *= ct
    b *= ct
    
    projectWBackInConvexSet
    this
  }  
  override def clone : ScorerWeights = {
    val rez = new ScorerWeights
    rez.a = a 
    rez.f = f 
    rez.g = g
    rez.h = h
    rez.s = s
    rez.b = b
    rez.sum_grad_a = sum_grad_a 
    rez.sum_grad_f = sum_grad_f 
    rez.sum_grad_g = sum_grad_g
    rez.sum_grad_h = sum_grad_h
    rez.sum_grad_s = sum_grad_s 
    rez.sum_grad_b = sum_grad_b 
    rez.squares_grad_a = squares_grad_a 
    rez.squares_grad_f = squares_grad_f 
    rez.squares_grad_g = squares_grad_g
    rez.squares_grad_h = squares_grad_h
    rez.squares_grad_s = squares_grad_s
    rez.squares_grad_b = squares_grad_b
    rez
  }
  def print = {
    println("Weights: a = " + a + "; f = " + f + "; g = " + g + "; h = " + h + "; s = " + s + "; b = " + b + " num grads = " + num_grads)
    println("Square Gradient sums: a = " + squares_grad_a + "; f = " + squares_grad_f +  "; g = " + squares_grad_g + 
        "; h = " + squares_grad_h + "; s = " + squares_grad_s  + "; b = " + squares_grad_b)
    println("Gradient sums: a = " + sum_grad_a + "; f = " + sum_grad_f + "; g = " + sum_grad_g + "; h = " + sum_grad_h +
        "; s = " + sum_grad_s + "; b = " + sum_grad_b)
  }
  def projectWBackInConvexSet() = {
    if (g <= 0.001) {
      g = 0.001
    }
    if (g > 15) {
      g = 15
    }
    if (f <= 0.001) {
      f = 0.001
    }
    if (f > 15) {
      f = 15
    }    
    if (a <= 0.00000001) {
      a = 0.00000001
    }
    if (a > 10) {
      a = 10
    }
    if (s <= 0.00000001) {
      s = 0.00000001
    }
    if (s > 10) {
      s = 10
    }

    if (b <= 0.00000001) {
      b = 0.00000001
    }
    if (b > 10) {
      b = 10
    }

    if (h * g < -15) {
      h = -15.0 / g
    }
  }
}