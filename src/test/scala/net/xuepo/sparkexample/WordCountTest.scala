package net.xuepo.sparkexample

import org.testng.annotations.Test
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xuepo on 4/3/16.
  */

@Test
class WordCountTest {
  @Test
  def WordCountTest(): Unit = {
    val lines = Seq("foo bar foo foo bar bar foo bar",
      "there are lots of foos and bars",
      "foo, bar")

    val conf = new SparkConf()
      .setAppName("Spark Example")
      .setMaster("local[2]")
    val sparkContext = new SparkContext(conf)
    val input =sparkContext.parallelize(lines)
    val result: Map[String, Int] = WordCount.countWords(input).collect().toMap;

    for (kv <- result) {
      println(kv._1 + " : " + kv._2)
    }
  }
}
