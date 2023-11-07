package Abgabe1

import java.sql.DriverManager
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object JDBCQueries {
  def apply(driver: String, url: String): JDBCQueries = new JDBCQueries(driver, url)
}

class JDBCQueries(driver: String, url: String) extends SimpleQueries {
  private val connection = DriverManager.getConnection(url, "sa", "123")
  private val countGoalsQuery = connection.prepareStatement("SELECT COUNT(*) FROM GOALSCORERS_FILE WHERE scorer = ?;")
  private val countRangeQuery = connection.prepareStatement("SELECT COUNT(*) FROM RESULT_FILE WHERE (home_score + away_score) >= ? AND (home_score + away_score) <= ?;")
  private val isConsistentQuery = connection.prepareStatement(
    "SELECT g.date, g.home_team, g.away_team, g.team, count(g.team), r.home_score, r.away_score FROM GOALSCORERS_FILE  as g " +
    "INNER JOIN RESULT_FILE as r on g.date = r.date and g.home_team = r.home_team and g.away_team = r.away_team " +
    "GROUP BY g.date, g.home_team, g.away_team, g.team " +
    "HAVING ((g.home_team = g.team) and (count(g.team) != r.home_score)) or ((g.away_team = g.team) and (count(g.team) != r.away_score))")

  override def close(): Future[Unit] = Future {
    isConsistentQuery.close()
    countGoalsQuery.close()
    countRangeQuery.close()
    connection.close()
  }

  override def isConsistent(): Future[Boolean] = Future {
    val query = isConsistentQuery.executeQuery()
    !query.next()
  }

  override def countGoals(name: String): Future[Int] = Future {
    countGoalsQuery.setString(1, name)
    val count = countGoalsQuery.executeQuery()
    count.next()
    count.getInt(1)
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = Future {
    countRangeQuery.setInt(1, min)
    countRangeQuery.setInt(2, max)
    val count = countRangeQuery.executeQuery()
    count.next()
    count.getInt(1)
  }
}