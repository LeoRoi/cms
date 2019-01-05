package object common {

  def ??? : Nothing = throw new Error("an implementation is missing")
  
  type ??? = Nothing
  
  type *** = Any

	
}



package object conc {
  
  def thread(body: => Unit):Thread= {
    
    val t= new Thread{
      
      override def run= body
    }
    t.start
    t
  }
}

