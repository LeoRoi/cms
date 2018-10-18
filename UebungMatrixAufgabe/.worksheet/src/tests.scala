object tests {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(58); 
  println("Welcome to the Scala worksheet");$skip(9); val res$0 = 
  0 to 5;System.out.println("""res0: scala.collection.immutable.Range.Inclusive = """ + $show(res$0));$skip(10); val res$1 = 
  0 to -1;System.out.println("""res1: scala.collection.immutable.Range.Inclusive = """ + $show(res$1));$skip(24); 
  
  val a=Array(1,2,3);System.out.println("""a  : Array[Int] = """ + $show(a ));$skip(88); 
  
  def vecMul(v1:Array[Int],v2:Array[Int]):Int=
    v1 zip v2 map (X=> X._1*X._2) sum;System.out.println("""vecMul: (v1: Array[Int], v2: Array[Int])Int""");$skip(17); val res$2 = 
  
  vecMul(a,a);System.out.println("""res2: Int = """ + $show(res$2))}
  
}
