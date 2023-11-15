package Abgabe2

import Abgabe2.Main.{host, port}
import redis.clients.jedis.{JedisPooled, Pipeline}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

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
      val home_score = pipeline.hget(resultKey, "home_score")
      val away_score = pipeline.hget(resultKey, "away_score")
      pipeline.sync()

      home_goals.get().toArray.length == home_score.get().toInt && away_goals.get().toArray.length == away_score.get().toInt
    }
    pipeline.close()
    consistent
  }

  override def countGoals(name: String): Future[Int] = Future {
    val keys = jedis.smembers("goal_ids").toArray.map(_.asInstanceOf[String]).toSet
    keys.count { key =>
      val scorer_name = jedis.hget(key, "scorer")
      name.equals(scorer_name)
    }
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = Future {
    val keys = jedis.smembers("result_ids").toArray.map(_.asInstanceOf[String]).toSet
    keys.count { key =>
      val home_score = jedis.hget(key, "home_score").toInt
      val away_score = jedis.hget(key, "away_score").toInt
      val combined = home_score + away_score
      min <= combined && combined <= max
    }
  }
}
