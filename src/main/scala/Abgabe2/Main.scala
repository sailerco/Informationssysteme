package Abgabe2

import redis.clients.jedis.Jedis

import java.sql.DriverManager
import scala.io.Source
import scala.util.control.Breaks.break

object Main {
  private val url = "jdbc:h2:/localhost/~/DB-IS;AUTO_SERVER=TRUE"

  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 6379
    val jedis = new Jedis(host, port)
    //generateGamesAndResults(jedis)
    generateGoals(jedis)
    jedis.close()
  }

  def generateGoals(jedis: Jedis): Unit = {
    val goals = Source.fromFile("C:/Users/cocos/Desktop/goalscorers.csv")
    val keys = jedis.keys("game:*").toArray.map(_.asInstanceOf[String]).toSet
    println(goals.getLines().next())
    val iterator = goals.getLines().drop(1).zipWithIndex
    for ((row, index) <- iterator) {
      val goalHashKey = s"goal:" + index
      val fields = row.split(",")

      val game_id = keys.find{key =>
        val date = jedis.hget(key, "date")
        val home_team = jedis.hget(key, "home_team_id").split(":")(1)
        val away_team = jedis.hget(key, "away_team_id").split(":")(1)
        fields(0) == date && fields(1) == home_team && fields(2) == away_team
      }

      jedis.hset(goalHashKey, "game_id", game_id.get)

      if (!jedis.sismember("team_set", s"team:${fields(3)}"))
        jedis.sadd("team_set", s"team:${fields(3)}")
      jedis.hset(goalHashKey, "team_id", s"team:${fields(3)}")

      if (!jedis.sismember("scorer_set", s"scorer:${fields(4)}")) {
        jedis.sadd("scorer_set", s"scorer:${fields(4)}")
      }
      jedis.hset(goalHashKey, "scorer_id", s"scorer:${fields(4)}")

      jedis.hset(goalHashKey, "minute", fields(5))
      jedis.hset(goalHashKey, "own_goal", fields(6))
      jedis.hset(goalHashKey, "penalty", fields(7))

    }
  }

  def generateGamesAndResults(jedis: Jedis): Unit = {
    val games = Source.fromFile("C:/Users/cocos/Desktop/results.csv")
    val iterator = games.getLines().drop(1).zipWithIndex
    for ((row, index) <- iterator) {
      val gamesHashKey = s"game:" + index
      val resultsHashKey = s"results:" + index
      val fields = row.split(",")
      jedis.hset(gamesHashKey, "date", fields(0))

      if (!jedis.sismember("team_set", s"team:${fields(1)}"))
        jedis.sadd("team_set", s"team:${fields(1)}")
      if (!jedis.sismember("team_set", s"team:${fields(2)}"))
        jedis.sadd("team_set", s"team:${fields(2)}")

      jedis.hset(gamesHashKey, "home_team_id", s"team:${fields(1)}")
      jedis.hset(gamesHashKey, "away_team_id", s"team:${fields(2)}")

      jedis.hset(resultsHashKey, "home_score", fields(3))
      jedis.hset(resultsHashKey, "away_score", fields(4))
    }
  }
}
