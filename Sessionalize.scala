/* Sessionalize.scala */
import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Sessionalize {

  //auxilary
  private object MD5{
    def hash(s:String)={
      val m = java.security.MessageDigest.getInstance("MD5")
      val b = s.getBytes("UTF-8")
      m.update(b,0,b.length)
      new java.math.BigInteger(1,m.digest()).toString(16)
    }
  }

  //RDD for question 3
  private def rdd003(log:RDD[String]):RDD[(String, Iterable[(Long, String)])]={
    val iptime = log.map(x => {
      val s = x.split(" ")
      //parse visiter, get rid of port number
      val ip = s(2).split(":")(0)
      //parse url
      val url = MD5.hash(s(12))
      //parse total spend time
      val time : Double = s(4).toDouble+s(5).toDouble+s(6).toDouble
      //parse timestamp
      val tz = TimeZone.getTimeZone("UTC");
      val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")//The time when the load balancer received the request from the client, in ISO 8601 format
      df.setTimeZone(tz);
      val timestamp = df.parse(s(0));
      (ip, (timestamp.getTime(), url))
    }).sortBy(_._2._1, true).groupByKey()
    return iptime
  }

  //RDD for question 1,2,4
  private def rdd124(log: RDD[String]): RDD[(String, Iterable[Long])]={
    val iptime = log.map(x => {
      val s = x.split(" ")
      //parse visiter, get rid of port number
      val ip = s(2).split(":")(0)
      //parse url
      val url = MD5.hash(s(12))
      //parse total spend time
      val time : Double = s(4).toDouble+s(5).toDouble+s(6).toDouble
      //parse timestamp
      val tz = TimeZone.getTimeZone("UTC");
      val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")//The time when the load balancer received the request from the client, in ISO 8601 format
      df.setTimeZone(tz);
      val timestamp = df.parse(s(0));
      (ip, timestamp.getTime())
    }).sortBy(_._2, true).groupByKey()
    return iptime
  }


  def main(args: Array[String]) {
    val logFile = "data/2015_07_22_mktplace_shop_web_log_sample.log.gz" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val log = sc.textFile(logFile)

    //execute
    question1(log);
    question2(log);
    question3(log);
    question4(log);

  }

  /**
    * 1.Sessionize the web log by IP. Key is ip, value is the total session number respectively
    *
    * val result1: "Array[(String, Int)] = Array((27.97.100.77,1), (103.15.250.10,3)..."
    * String part is the ip adress from clients, Int part means how many sessions this client has
    */
  def question1(log:RDD[String]) {
    val result1 = rdd124(log).mapValues(x => {
      var old : Long = 0
      var session = 0
      x.foreach(date => {
        if(date > old+15*60*1000) //15min window limit
          session = session + 1
        old = date;
      })
      session
    })
  }

  /**
    * 2.Determine the average session time
    *
    * val result2: "org.apache.spark.rdd.RDD[Long]"
    * the Long means the total accumulated session duration for each client perspectively
    */
  def question2(log:RDD[String]) : Double={
    val result2 = rdd124(log).mapValues(x => {
      var previous : Long = 0
      var session = 0
      var duration : Long = 0
      var sessionBegin : Long = 0;
      x.foreach(date => {
        if(date > previous+15*60*1000){//15min window limit
          session = session + 1
          duration += previous - sessionBegin
        }else{
          duration += date - sessionBegin
        }
        sessionBegin = date;
        previous = date;
      })
      duration/session
    }).values.cache()
    val ipNumber = result2.count()
    val totalDuration = result2.reduce((x,y)=>x+y)
    return totalDuration/ipNumber
  }

  /**
    * 3.Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    *
    * val result3: Array[(String, scala.collection.mutable.HashMap[Int,Int])] = Array((27.97.100.77,Map(1 -> 1)), (103.15.250.10,Map(2 -> 1, 1 -> 1, 3 -> 3))...
    * the String part means the client ip
    * the first Int of the HashMap means session id of this client
    * the second Int of the HashMap means distinct url accessed for the specified session id of that client
    */
  def question3(log:RDD[String]) {
    val result3 = rdd003(log).mapValues(x => {
      var old : Long = 0
      var session = 0
      val urlCount = scala.collection.mutable.ListBuffer.empty[String]
      val map = new mutable.HashMap[Int, Int]
      x.foreach(dateUrl => {
        var date = dateUrl._1
        var url = dateUrl._2
        urlCount += url
        if(date > old+15*60*1000){ //15min window limit
          session = session + 1
          map += session -> urlCount.distinct.size
          urlCount.clear()
        }
        old = date;
      })
      map
    })
  }

  /**
    * 4.Find the most engaged users, ie the IPs with the longest session times
    *
    * val result4: org.apache.spark.rdd.RDD[(String, Long)]
    * the String part means client ip
    * the Long part means the total accumulated session duration time for every client
    */
  def question4(log:RDD[String]):List[String]={
    val result4 = rdd124(log).mapValues(x => {
      var previous : Long = 0
      var session = 0
      var duration : Long = 0
      var sessionBegin : Long = 0;
      x.foreach(date => {
        if(date > previous+15*60*1000){//15min window limit
          session = session + 1
          duration += (previous - sessionBegin);
        }else{
          duration += date - sessionBegin
        }
        sessionBegin = date;
        previous = date;
      })
      duration
    }).sortBy(_._2, false).cache()

    val maxDuration = result4.take(1)(0)._2
    val ips = mutable.ListBuffer.empty[String]
    result4.foreach(x => {
      var ip = x._1
      var duration = x._2
      if (duration == maxDuration) {
        ips += ip
      } else {
        //break
      }
    })
    return ips.toList
  }

}

