import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when,sum,count,avg}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object Ram {
  def main(args:Array[String]):Unit=
  {
    //
    //    val spark=SparkSession.builder()
    //      .appName("spark-program")
    //      .master("local[*]")
    //      .getOrCreate()


    val sparkconf=new SparkConf()
    sparkconf.set("spark.app.master","spark-program")
    sparkconf.set("spark.master","local[*]")

    val spark=SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()



    //    val schema=" id Int, name String, age Int"




    val schema =StructType(List(

      StructField("id",IntegerType),
      StructField("Name",StringType),
      StructField("Salary",IntegerType),
      StructField("City",StringType)

    ))

    val df=spark.read
      .format("csv")
      .option("header",true)
      .schema(schema)
      .option("path","C:/Users/Shivakumar Koti/Downloads/details.csv")
      .load()



    //     df.select(
    //
    //       col("id"),
    //       col("Name"),
    //       col("Salary"),
    //       col("City"),
    //       when(col("Salary")>800,"rich").when(col("Salary")>400 && col("Salary")<=800,"middle").otherwise("poor").alias("status")
    //     ).show()



    //    df.createTempView("karthik")
    //
    //    spark.sql(
    //      """
    //        select
    //         id,
    //         Name,
    //         Salary,
    //         City,
    //         Case
    //         when Salary>800 Then "RICH"
    //         when Salary>400 AND Salary<=800 then "middle"
    //         else "poor"
    //         end as status
    //         from
    //         karthik
    //        """
    //    ).show()


    // val input=sc.textFile("C:/Users/Karthik Kondpak/Desktop/apr/data.txt")
    //    val rdd1=input.flatMap(x=>x.split(" "))
    //    val rdd2=rdd1.map(x=>(x,1))
    //    val rdd3=rdd2.reduceByKey((x,y)=>x+y)
    //  val rdd4=rdd3.sortBy(x=>x._2,false)
    //    rdd3.collect.foreach(println)

    //convert aray data ---into dataframe/rdd

    //    val arr=Array(10,20,30,40,50,60,70,80,91)
    //
    //    val rdd1=sc.parallelize(arr)
    //
    //    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5))
    //    rdd.saveAsTextFile("C:/Users/Karthik Kondpak/Desktop/apr/oct26")

    //    val rdd1 = sc.parallelize(Array("apple", "banana", "carrot"))
    //
    //    val search="carr"
    //
    //    val rdd2=rdd1.filter(x=>x.contains(search))
    //
    //     rdd2.collect.foreach(println)
    //
    //
    //
    //
    //
    //
    //
    //
    //scala.io.StdIn.readLine()
    /// exercisse1
//    import spark.implicits._
//    val list=List((1,"smartphone",700,"Electronics"),(2,"TV",1200,"Electronics"),(3,"shoes",120,"apparel")).toDF("product_id","product","price","category")
//
//    list.select(col("product_id"),col("product"),col("category"),col("price"),when(col("price")>500,"expensive").otherwise("non-expensive")).show()
//    //    list.select(col("id"),col("name").startsWith("v")).show()
//
//    list.filter(col("product")endsWith("s")).show()

    ///  Exercise 2
//    import spark.implicits._
//    val list=List((1,"The Matrix",9, 136),(2,"Inception",8, 148),
//      (3,"The Godfather",9, 175),(4,"Toy Story",7, 81),
//      (5,"The Shawshank Redemption",10, 142),(6,"The Silence of the Lambs",8, 118)).
//      toDF("movie_id","movie_name","rating","duration_minutes")
//    list.select(col("movie_id"),col("movie_name"),col("rating"),col("duration_minutes"))
//      .withColumn("rating_category",when(col("rating")>=8, "Excellent")
//        .when(col("rating")>=6 && col("rating") <=8, "Good").otherwise("Average"))
//      .withColumn("duration_category", when(col("duration_minutes")> 150, "Long")
//        .when(col("duration_minutes")>=90 && col("duration_minutes") <=150, "Medium").otherwise("Short"))
//      .filter(col("movie_name").startsWith("T"))
//      .filter(col("movie_name").endsWith("e")).show()


    //  Exercise 3 aggregate function
//    import spark.implicits._
//
//    val orderData = List(
//      ("Order1", "John", 100),
//      ("Order2", "Alice", 200),
//      ("Order3", "Bob", 150),
//      ("Order4", "Alice", 300),
//      ("Order5", "Bob", 250),
//      ("Order6", "John", 400)
//    ).toDF("OrderID", "Customer", "Amount")
//
//
//    orderData.groupBy("customer").agg(count(col("OrderID")),sum(col("Amount"))).show()

    val scoreData = List(
      ("Alice", "Math",80),
      ("Bob", "Math",90),
      ("Alice", "Science",70),
      ("Bob", "Science",85),
      ("Alice", "English",75),
      ("Bob", "English",95)).toDF("Student", "Subject", Score)

    scoreData.select(avg(col("Score"))).show()

  }


}

//import org.apache.spark.SparkContext
//import java.time.{Instant, Duration}
//
//object Hello {
//  def main(args: Array[String]): Unit = {
//    // Initialize SparkContext
//    val sc = new SparkContext("local[4]", "Karthik")
//
//    // Benchmarking file reading
//    val readStart = Instant.now()
//    val input = sc.textFile("C:/Users/Karthik Kondpak/Desktop/apr/data.txt")
//    val readDuration = Duration.between(readStart, Instant.now()).toMillis
//    println(s"File reading time: $readDuration ms")
//
//    // Benchmarking word splitting
//    val splitStart = Instant.now()
//    val rdd1 = input.flatMap(x => x.split(" "))
//    val splitDuration = Duration.between(splitStart, Instant.now()).toMillis
//    println(s"Word splitting time: $splitDuration ms")
//
//    // Benchmarking mapping words to counts
//    val mapStart = Instant.now()
//    val rdd2 = rdd1.map(x => (x, 1))
//    val mapDuration = Duration.between(mapStart, Instant.now()).toMillis
//    println(s"Mapping time: $mapDuration ms")
//
//    // Benchmarking word count aggregation (reduceByKey)
//    val reduceStart = Instant.now()
//    val rdd3 = rdd2.reduceByKey((x, y) => x + y)
//    val reduceDuration = Duration.between(reduceStart, Instant.now()).toMillis
//    println(s"Reduce (word count) time: $reduceDuration ms")
//
//    // Benchmarking sorting
//    val sortStart = Instant.now()
//    val rdd4 = rdd3.sortBy(x => x._2, false)
//    val sortDuration = Duration.between(sortStart, Instant.now()).toMillis
//    println(s"Sorting time: $sortDuration ms")
//
//    // Print top 2 words as final result
//    rdd3.take(2).foreach(println)
//
//    // Pause execution for review
//    scala.io.StdIn.readLine()
//
//    // Stop SparkContext
//    sc.stop()
//  }
//}