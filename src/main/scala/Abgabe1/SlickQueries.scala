package Abgabe1

import slick.jdbc.H2Profile.api._
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object SlickQueries {
  def apply(driver: String, url: String): SlickQueries = new SlickQueries(driver, url)
}

class SlickQueries(driver: String, url: String) extends SimpleQueries {
  private val db = Database.forURL(url, "sa", "123", driver = driver)
  private val results = TableQuery[Results]
  private val goalscorers = TableQuery[Goalscorers]

  override def close(): Future[Unit] = Future {
    db.close()
  }

  override def isConsistent(): Future[Boolean] = {
    //join results and goalscorers and group it. group length returns the number of rows that were grouped.
    //will give us a Sequence of Tuple
    val query = (for {
      (g, r) <- goalscorers join results on ((g, r) => g.date === r.date && g.homeTeam === r.homeTeam && g.awayTeam === r.awayTeam)
    } yield (g.date, g.homeTeam, g.awayTeam, g.team, r.homeScore, r.awayScore))
      .groupBy(col => (col._1, col._2, col._3, col._4, col._5, col._6))
      .map { case (col, group) => (col, group.length) }
      .result

    //check if the entries are consistent (query.map = FlatMap in Slick)
    val query_action = query.map(rows => {
      val check_consistency = rows.map(entries => entries._1._4 match {
        case entries._1._2 => entries._2 == entries._1._5 //if winner == home_team compare group length and home_score
        case entries._1._3 => entries._2 == entries._1._6 //if winner == away_team compare group length and away_score
        case _ => false
      })
      //check if one value was false, and return result to query_action
      !check_consistency.contains(false)
    })

    db.run(query_action)
  }

  override def countGoals(name: String): Future[Int] = {
    val query = goalscorers.filter(_.scorer === name).length
    db.run(query.result)
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = {
    val query = results.filter(
      row => (row.homeScore + row.awayScore) >= min
        && (row.homeScore + row.awayScore) <= max).length
    db.run(query.result)
  }
}