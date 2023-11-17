package Abgabe2

import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.{JedisPooled, Pipeline}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.ListHasAsScala

object JedisQueries {
  def apply(host: String, port: Int): JedisQueries = new JedisQueries(host, port)
}

class JedisQueries(host: String, port: Int) extends SimpleQueries {
  val jedis = new JedisPooled(host, port)
  val pipeline: Pipeline = jedis.pipelined()

  override def close(): Future[Unit] = Future {
    pipeline.close()
    jedis.close()
  }

  override def isConsistent(): Future[Boolean] = Future {
    val pipeline = jedis.pipelined()
    val goalIds = jedis.smembers("goal_ids").toArray.map(_.asInstanceOf[String]).toSet
    val consistent = goalIds.forall { key =>
      val id = jedis.hget(key, "results_id")
      val resultKey = s"results:$id"

      val home_goals = pipeline.zrangeByScore("home_goals", id, id)
      val away_goals = pipeline.zrangeByScore("away_goals", id, id)
      val result = pipeline.hgetAll(resultKey)
      pipeline.sync()
      home_goals.get().toArray.length == result.get().get("home_score").toInt && away_goals.get().toArray.length == result.get().get("away_score").toInt
    }
    pipeline.close()
    consistent
  }

  override def countGoals(name: String): Future[Int] = Future {
    jedis.zscore("Scorer", name).toInt
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = Future {
    val keys = jedis.zrangeWithScores("TotalGoals", 0, -1).asScala.toList
    keys
      .filter(k => (min to max).contains(k.getElement.toInt))
      .map(_.getScore).sum.toInt
  }
}
