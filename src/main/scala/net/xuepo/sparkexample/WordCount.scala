package net.xuepo.sparkexample

import org.apache.spark.rdd.RDD

/**
  * Created by xuepo on 4/3/16.
  */

object WordCount {
  def countWords(input: RDD[String]): RDD[(String, Int)] = {
    input
      .flatMap(line => line.split("\\s+"))
      .filter(_.length > 0)
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
  }
}
