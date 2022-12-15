case class Neumaier(sum: Double, c: Double)

object HW {

   def q1_countsorted(x: Int, y: Int, z:Int) = {
      i = 0
      val a = if x < y{
          i++
          }
      else if y < z{
          i++
          }
      else if x < z{
          i++
          }
   }


   def q2_interpolation(name: String, age: Int) = {
      if age <= 21{
         print("Hello", name)
      }
      else if age > 21{
         print("Howdy", name)
      }
   }


   def q3_polynomial(arr: Seq[Double]): Double = {
      arr.foldLeft(0.0)((acc, x) => acc + x * (acc + 1))
   }

   
   def q4_application(x: Int, y: Int, z: Int)(f: (Int, Int) => Int) = {

      1
   }


   def q5_stringy(start: Int, n: Int): Vector[String] = {
      for (i <- 0 until n) yield (start + i).toString
          Vector.tabulate(n)(i => (start + i).toString)
   }


   def q6_modab(a: Int, b: Int, c: Seq[Int]) = Seq[Int]{
      val result = c.filter(_ < a)
      val result2 = result.filter(b % _ != 0)
      result2
   }


   def q7_count(arr: Vector[Int])(f: (Int, Int) => Boolean) = {
      if f(arr.head == true) 
      {1 + q7_count(arr.tail, f())}
   }


   def q8_count_tail(){

   }

   def q9_neumaier(){
function NeumaierSum(input)
    var sum = 0.0
   var c = 0.0
   for i = 1 to input.length do
       var t = sum + input[i]
       if |sum| >= |input[i]| then
           c += (sum - t) + input[i]
      else
          c += (input[i] - t) + sum 
      endif
      sum = t
   return sum + c
   }

}
