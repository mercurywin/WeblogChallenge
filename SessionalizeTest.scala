import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.junit.AssertionsForJUnit

import org.junit.Assert._
import org.junit._

class SessionalizeTest extends AssertionsForJUnit {

  var sc: SparkContext = _
  val DELTA = 1e-15;

  @Before def initialize() {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    sc = new SparkContext(conf)
  }

  @After def destroy(): Unit ={
    sc.stop()
  }

  @Test def question2_1() {
    val data = Array(
      "2015-07-22T02:43:19.555732Z marketpalce-shop 1.38.22.212:15877 10.0.4.225:80 0.000023 0.004301 0.000021 200 200 0 13820 \"GET https://paytm.com:443/shop/h/electronics?utm_term=1028 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T02:45:19.555732Z marketpalce-shop 1.38.22.212:53342 10.0.6.99:80 0.000023 0.004062 0.000022 200 200 0 11213 \"GET https://paytm.com:443/shop/summary/1115030398 HTTP/1.1\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) GSA/6.0.51363 Mobile/11D257 Safari/9537.53\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"
    )
    val log = sc.parallelize(data)
    assertEquals(120000, Sessionalize.question2(log), DELTA)
  }

  @Test def question2_2() {
    val data = Array(
      "2015-07-22T02:45:19.555732Z marketpalce-shop 1.38.22.212:15877 10.0.4.225:80 0.000023 0.004301 0.000021 200 200 0 13820 \"GET https://paytm.com:443/shop/h/electronics?utm_term=1028 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T12:43:19.654003Z marketpalce-shop 103.29.159.61:26631 10.0.6.199:80 0.000025 0.007135 0.000021 200 200 0 216 \"GET https://paytm.com:443/shop/cart?channel=web&version=2 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; rv:25.0) Gecko/20100101 Firefox/25.0\" ECDHE-RSA-AES128-SHA TLSv1",
      "2015-07-22T02:43:19.552777Z marketpalce-shop 37.228.105.137:47348 10.0.6.195:80 0.000028 0.000947 0.000021 200 200 0 714 \"GET https://paytm.com:443/offer/wp-content/plugins/really-simple-facebook-twitter-share-buttons/images/specificfeeds_follow.png HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/10.0.1884/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1",
      "2015-07-22T02:43:19.555732Z marketpalce-shop 1.38.22.212:53342 10.0.6.99:80 0.000023 0.004062 0.000022 200 200 0 11213 \"GET https://paytm.com:443/shop/summary/1115030398 HTTP/1.1\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) GSA/6.0.51363 Mobile/11D257 Safari/9537.53\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"
    )
    val log = sc.parallelize(data)
    assertEquals(40000, Sessionalize.question2(log), DELTA)
  }

  @Test def question2_3() {
    val data = Array(
      "2015-07-22T02:45:19.555732Z marketpalce-shop 1.38.22.212:15877 10.0.4.225:80 0.000023 0.004301 0.000021 200 200 0 13820 \"GET https://paytm.com:443/shop/h/electronics?utm_term=1028 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T12:43:19.654003Z marketpalce-shop 103.29.159.61:26631 10.0.6.199:80 0.000025 0.007135 0.000021 200 200 0 216 \"GET https://paytm.com:443/shop/cart?channel=web&version=2 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; rv:25.0) Gecko/20100101 Firefox/25.0\" ECDHE-RSA-AES128-SHA TLSv1",
      "2015-07-22T02:43:19.552777Z marketpalce-shop 37.228.105.137:47348 10.0.6.195:80 0.000028 0.000947 0.000021 200 200 0 714 \"GET https://paytm.com:443/offer/wp-content/plugins/really-simple-facebook-twitter-share-buttons/images/specificfeeds_follow.png HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/10.0.1884/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1",
      "2015-07-22T02:43:19.555732Z marketpalce-shop 1.38.22.212:53342 10.0.6.99:80 0.000023 0.004062 0.000022 200 200 0 11213 \"GET https://paytm.com:443/shop/summary/1115030398 HTTP/1.1\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) GSA/6.0.51363 Mobile/11D257 Safari/9537.53\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T02:41:19.552777Z marketpalce-shop 37.228.105.137:47348 10.0.6.195:80 0.000028 0.000947 0.000021 200 200 0 714 \"GET https://paytm.com:443/offer/wp-content/plugins/really-simple-facebook-twitter-share-buttons/images/specificfeeds_follow.png HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/10.0.1884/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1"
    )
    val log = sc.parallelize(data)
    assertEquals(80000, Sessionalize.question2(log), DELTA)
  }

  @Test def question2_4() {
    val data = Array(
      "2015-07-22T02:45:19.555732Z marketpalce-shop 1.38.22.212:15877 10.0.4.225:80 0.000023 0.004301 0.000021 200 200 0 13820 \"GET https://paytm.com:443/shop/h/electronics?utm_term=1028 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T02:43:19.555732Z marketpalce-shop 1.38.22.212:53342 10.0.6.99:80 0.000023 0.004062 0.000022 200 200 0 11213 \"GET https://paytm.com:443/shop/summary/1115030398 HTTP/1.1\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) GSA/6.0.51363 Mobile/11D257 Safari/9537.53\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T03:10:19.552777Z marketpalce-shop 1.38.22.212:47348 10.0.6.195:80 0.000028 0.000947 0.000021 200 200 0 714 \"GET https://paytm.com:443/offer/wp-content/plugins/really-simple-facebook-twitter-share-buttons/images/specificfeeds_follow.png HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/10.0.1884/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1"
    )
    val log = sc.parallelize(data)
    assertEquals(60000, Sessionalize.question2(log), DELTA)
  }

  @Test def question2_5() {
    val data = Array(
      "2015-07-22T02:45:19.555732Z marketpalce-shop 1.38.22.212:15877 10.0.4.225:80 0.000023 0.004301 0.000021 200 200 0 13820 \"GET https://paytm.com:443/shop/h/electronics?utm_term=1028 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T02:43:19.555732Z marketpalce-shop 1.38.22.212:53342 10.0.6.99:80 0.000023 0.004062 0.000022 200 200 0 11213 \"GET https://paytm.com:443/shop/summary/1115030398 HTTP/1.1\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) GSA/6.0.51363 Mobile/11D257 Safari/9537.53\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T03:10:19.552777Z marketpalce-shop 1.38.22.212:47348 10.0.6.195:80 0.000028 0.000947 0.000021 200 200 0 714 \"GET https://paytm.com:443/offer/wp-content/plugins/really-simple-facebook-twitter-share-buttons/images/specificfeeds_follow.png HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/10.0.1884/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1",
      "2015-07-22T03:12:19.552777Z marketpalce-shop 1.38.22.212:47348 10.0.6.195:80 0.000028 0.000947 0.000021 200 200 0 714 \"GET https://paytm.com:443/offer/wp-content/plugins/really-simple-facebook-twitter-share-buttons/images/specificfeeds_follow.png HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/10.0.1884/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1"
    )
    val log = sc.parallelize(data)
    assertEquals(120000, Sessionalize.question2(log), DELTA)
  }

  @Test def question2_6() {
    val data = Array(
      "2015-07-22T02:45:19.555732Z marketpalce-shop 1.38.22.212:15877 10.0.4.225:80 0.000023 0.004301 0.000021 200 200 0 13820 \"GET https://paytm.com:443/shop/h/electronics?utm_term=1028 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T02:43:19.555732Z marketpalce-shop 1.38.22.212:53342 10.0.6.99:80 0.000023 0.004062 0.000022 200 200 0 11213 \"GET https://paytm.com:443/shop/summary/1115030398 HTTP/1.1\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) GSA/6.0.51363 Mobile/11D257 Safari/9537.53\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T03:12:19.552777Z marketpalce-shop 1.38.22.212:47348 10.0.6.195:80 0.000028 0.000947 0.000021 200 200 0 714 \"GET https://paytm.com:443/offer/wp-content/plugins/really-simple-facebook-twitter-share-buttons/images/specificfeeds_follow.png HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/10.0.1884/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1",
      "2015-07-22T03:28:19.552777Z marketpalce-shop 1.38.22.212:47348 10.0.6.195:80 0.000028 0.000947 0.000021 200 200 0 714 \"GET https://paytm.com:443/offer/wp-content/plugins/really-simple-facebook-twitter-share-buttons/images/specificfeeds_follow.png HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/10.0.1884/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1"
    )
    val log = sc.parallelize(data)
    assertEquals(40000, Sessionalize.question2(log), DELTA)
  }

  @Test def question4_1() {
    val data = Array(
      "2015-07-22T02:45:19.555732Z marketpalce-shop 1.38.22.212:15877 10.0.4.225:80 0.000023 0.004301 0.000021 200 200 0 13820 \"GET https://paytm.com:443/shop/h/electronics?utm_term=1028 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T02:43:19.555732Z marketpalce-shop 1.38.22.212:53342 10.0.6.99:80 0.000023 0.004062 0.000022 200 200 0 11213 \"GET https://paytm.com:443/shop/summary/1115030398 HTTP/1.1\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) GSA/6.0.51363 Mobile/11D257 Safari/9537.53\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"
    )
    val expected = List("1.38.22.212")
    val log = sc.parallelize(data)
    val actual = Sessionalize.question4(log)
    actual.foreach(ip => {
      assertTrue(expected.contains(ip))
    })
  }

  @Test def question4_2() {
    val data = Array(
      "2015-07-22T02:45:19.555732Z marketpalce-shop 1.38.21.212:15877 10.0.4.225:80 0.000023 0.004301 0.000021 200 200 0 13820 \"GET https://paytm.com:443/shop/h/electronics?utm_term=1028 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T02:43:19.555732Z marketpalce-shop 12.38.22.212:53342 10.0.6.99:80 0.000023 0.004062 0.000022 200 200 0 11213 \"GET https://paytm.com:443/shop/summary/1115030398 HTTP/1.1\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) GSA/6.0.51363 Mobile/11D257 Safari/9537.53\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T02:44:19.555732Z marketpalce-shop 12.38.22.212:47348 10.0.6.195:80 0.000028 0.000947 0.000021 200 200 0 714 \"GET https://paytm.com:443/offer/wp-content/plugins/really-simple-facebook-twitter-share-buttons/images/specificfeeds_follow.png HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/10.0.1884/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1",
      "2015-07-22T02:46:19.555732Z marketpalce-shop 1.38.21.212:47348 10.0.6.195:80 0.000028 0.000947 0.000021 200 200 0 714 \"GET https://paytm.com:443/offer/wp-content/plugins/really-simple-facebook-twitter-share-buttons/images/specificfeeds_follow.png HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/10.0.1884/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1"
    )
    val expected = List("1.38.22.212", "12.38.22.212")
    val log = sc.parallelize(data)
    val actual = Sessionalize.question4(log)
    actual.foreach(ip => {
      assertTrue(expected.contains(ip))
    })
  }

  @Test def question4_3() {
    val data = Array(
      "2015-07-22T02:45:19.555732Z marketpalce-shop 1.38.21.212:15877 10.0.4.225:80 0.000023 0.004301 0.000021 200 200 0 13820 \"GET https://paytm.com:443/shop/h/electronics?utm_term=1028 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T02:43:19.555732Z marketpalce-shop 12.38.22.212:53342 10.0.6.99:80 0.000023 0.004062 0.000022 200 200 0 11213 \"GET https://paytm.com:443/shop/summary/1115030398 HTTP/1.1\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) GSA/6.0.51363 Mobile/11D257 Safari/9537.53\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2",
      "2015-07-22T02:50:19.555732Z marketpalce-shop 12.38.22.212:47348 10.0.6.195:80 0.000028 0.000947 0.000021 200 200 0 714 \"GET https://paytm.com:443/offer/wp-content/plugins/really-simple-facebook-twitter-share-buttons/images/specificfeeds_follow.png HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/10.0.1884/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1",
      "2015-07-22T02:46:19.555732Z marketpalce-shop 1.38.21.212:47348 10.0.6.195:80 0.000028 0.000947 0.000021 200 200 0 714 \"GET https://paytm.com:443/offer/wp-content/plugins/really-simple-facebook-twitter-share-buttons/images/specificfeeds_follow.png HTTP/1.1\" \"Opera/9.80 (Android; Opera Mini/10.0.1884/37.6066; U; en) Presto/2.12.423 Version/12.16\" ECDHE-RSA-AES128-SHA TLSv1"
    )
    val expected = List("12.38.22.212")
    val log = sc.parallelize(data)
    val actual = Sessionalize.question4(log)
    actual.foreach(ip => {
      assertTrue(expected.contains(ip))
    })
  }

}