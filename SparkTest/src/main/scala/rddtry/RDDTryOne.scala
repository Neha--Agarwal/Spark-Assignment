import org.apache.spark.{SparkConf, SparkContext}

/**
  * First Spark Program
  * Created by neha on 17/1/17.
  */

object RDDTryOne{
  def main(args: Array[String]): Unit = {

    //local[*] takes all cores in the machine
    //local[1] takes 1 core and so on
    val conf= new SparkConf().setMaster("local[*]").setAppName("RDDTryOne")
    val sc= new SparkContext(conf)

    //Reading text file
    val ipPath= "/home/neha/Desktop/SparkTest/src/main/resources/dataframetry/warandpeace.txt"

    /*
      Source
      Creating an RDD from the file. This becomes the source.
      We are using cache() because 2 operations are being done. Count on whole then sampling
      and then its count. So to save time cache() is being done. No need to cache if only 1 operation
      was being done.
     */

    val readRDD= sc.textFile(ipPath).cache()

    val sampleRDD= readRDD.sample(withReplacement = false, 0.1, 123)
    //Count is action. Takes \n separated text as 1 sentence
    println("Total count: "+readRDD.count())
    println("Sample count: "+sampleRDD.count())

    /*
      Action
      Printing the output
      Displaying initial 10 lines
      Take() is action because it returns an array of strings
     */

    val xTake10= readRDD.take(10)
//  xTake10.foreach(println)

    /*
      withreplacement= false: When a random text is taken it is not kept back for next sample generation
      Will create array of strings
      Transformation. lazy
      Seed is used to get same output to that code can be testes
      0.1 is the percentage of text required in the sample
      y is an RDD so it's a transformation
    */
    val y= readRDD.sample(withReplacement = false, 0.1, 123)

    //Action
    //a is array of string so it's an action
    val z= readRDD.takeSample(withReplacement = false, 10, 123)

    readRDD.unpersist()

    //Transformation
    //Filter stop words

    //Splitting text into words to lowercase
    val splitRDD= readRDD.map(x=> x.toLowerCase.split(" ").toList)
    val opRDD= splitRDD.flatMap(x=>x).cache()

    //Removing stopwords
    val stopwordsandsymbols= List("the", "a", "an", "is", "are")
    println("Total output word count"+ opRDD.count())
    val filteredRDD = opRDD.filter(x=> !stopwordsandsymbols.contains(x))
    println("Filtered output word count"+ filteredRDD.count())
    filteredRDD.foreach(println)

    //RDD of individual word counts
    val wordUnitRDD= filteredRDD.map(x=> (x,1))
    val wordCountRDD= wordUnitRDD
      .groupBy(x=> x._1)
      .map(x=>{
        val key= x._1
        val totalCount= x._2.size
        ((key, totalCount))
      })
    wordCountRDD.foreach(println)

    //RDD of word frequency counts
    val freqCountRDD= wordCountRDD
      .groupBy(x=> x._2)
      .map(x=>{
        val key= x._1
        val totalCount= x._2.size
        ((key, totalCount))
      })
    freqCountRDD.foreach(println)

    //Coalesce RDD before sorting
    val tosort= wordCountRDD.coalesce(1)
    //Sorting word frequency RDD
    val sortedRDD= tosort.sortBy(x => x._2, ascending = false).coalesce(1)

//  sortedRDD.foreach(println)
    //Taking top 50% of word count RDD
    val half= (sortedRDD.count()/2).toInt
    val sortedHalfRDD= sortedRDD.take(half)
    sortedHalfRDD.foreach(x => println(x._1))
  }
}
