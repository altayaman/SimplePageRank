//package org.apache.spark.examples

import org.apache.spark.SparkContext._
//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 */
/*
If

the initial PageRank value for each webpage is 1.
page B has a link to pages C and A
page C has a link to page A
page D has links to all three pages


then input file has this content
    B C
    B A
    C A
    D A
    D B
    D C
*/
object SparkPageRank {

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {
    /* if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }
   */
    println("hello world")
    //showWarning()

    val sparkConf = new SparkConf().setAppName("PageRank")
      .setMaster("local")
    val iters = 10 //if (args.length > 0) args(1).toInt else 10
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile("/home/altay/Desktop/WebPageRelations.txt", 1)
    //val lines = ctx.textFile("hdfs://localhost:54310/user/hdfsHomeDir/WebPageRelations.txt") //("/home/altay/Desktop/WebPageRelations.txt", 1)
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    // Display the result of links

    var ranks = links.mapValues(v => 1.0)
    // Display the result of ranks

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }

      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    ranks.saveAsTextFile("/home/altay/Desktop/Result.txt")
    //val output = ranks.collect()
    //output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    ctx.stop()
  }
}