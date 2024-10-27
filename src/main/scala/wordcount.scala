import org.apache.spark.SparkContext

object third {
  def main(args:Array[String]):Unit= {

    val sc = new SparkContext("local[*]", "Shiva")
//    val input = sc.textFile("C:/Users/Shivakumar Koti/OneDrive/Desktop/Data2.txt")
//    val rdd1 = input.flatMap(x => x.split(" ")).filter(_.nonEmpty)
//    val rdd2 = rdd1.map(x => (x, 1))
//    val rdd3 = rdd2.reduceByKey((x, y) => x + y)
//    val rdd4 = rdd3.sortBy(x => x._2, false)
//    rdd4.take(2).foreach(println)

//    val arr = Array(10,20,30,40,50,60,70,80,91)
//    val rdd1 = sc.parallelize(arr)
//
////    val sum=rdd1.reduce((x,y)=>x+y)
////    val c = rdd1.count()
////    val mean= sum/c.toDouble
    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5))
    rdd.saveAsTextFile("C:/Users/Shivakumar Koti/OneDrive/Desktop/Oct20")
    val avg = rdd1.mean()
    print(mean)


  }
}