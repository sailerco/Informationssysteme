package Abgabe2

import redis.clients.jedis.{Jedis, JedisPooled}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.StdIn
import scala.util.{Failure, Success}


object Main {
  private val url = "jdbc:h2:/localhost/~/DB-IS;AUTO_SERVER=TRUE"

  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 6379
//    val jedis = new JedisPooled(host, port)
    val jedis = new Jedis(host, port)
    val pipeline = jedis.pipelined()

    //new GenerateData(pipeline).generateResults(pipeline)
    new GenerateData(jedis, pipeline).generate()
    /*val queries = new JedisQueries(jedis, pipeline)
    execute(queries, "Gerd Müller", 15, 20)
    StdIn.readLine()
    queries.close()*/
  }

  private def execute(queries: SimpleQueries, name: String, min: Int, max: Int): Unit = {
    queries.countGoals(name).onComplete {
      case Failure(exception) => exception.printStackTrace()
      case Success(value) => println(s"$name scored $value goals")
    }

    queries.isConsistent().onComplete {
      case Failure(exception) => exception.printStackTrace()
      case Success(true) => println("no inconsistent records in table 'goalscorers' found...")
      case Success(false) => println("inconsistent records in table 'goalscorers' found.")
    }

    queries.countRangeGoals(min, max).onComplete {
      case Failure(exception) => exception.printStackTrace()
      case Success(value) => println(s"number of games with at least $min and at most $max goals: $value")
    }
  }

}

