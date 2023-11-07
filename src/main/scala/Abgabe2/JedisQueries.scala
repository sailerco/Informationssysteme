package Abgabe2
import redis.clients.jedis.Jedis

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
object JedisQueries{
  def apply(jedis: Jedis): JedisQueries = new JedisQueries(jedis)
}
class JedisQueries(jedis: Jedis) extends SimpleQueries {
  override def close(): Future[Unit] = ???

  override def isConsistent(): Future[Boolean] = ???

  override def countGoals(name: String): Future[Int] = Future{
    val keys = jedis.keys("results:*").toArray.map(_.asInstanceOf[String]).toSet
    val count = keys.foldLeft(0) { (n, key) =>
      val home_score = jedis.hget(key, "home_score").toInt
      val away_score = jedis.hget(key, "away_score").toInt
      val combined_score = home_score + away_score
      if (15 <= combined_score && combined_score <= 20)
        n + 1
      else
        n
    }
    count
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = ???
}
