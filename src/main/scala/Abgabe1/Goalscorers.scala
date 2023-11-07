package Abgabe1

import slick.jdbc.H2Profile.api.{Table, Tag, _}

import java.sql.Date

class Goalscorers(tag: Tag) extends Table[(Date, String, String, String, String, Int, Boolean, Boolean)](tag, "GOALSCORERS_FILE") {
  def date = column[Date]("DATE")
  def homeTeam = column[String]("HOME_TEAM")
  def awayTeam = column[String]("AWAY_TEAM")
  def team = column[String]("TEAM")
  def scorer = column[String]("SCORER")
  def scoreMinute = column[Int]("SCORE_MINUTE")
  def ownGoal = column[Boolean]("OWN_GOAL")
  def penalty = column[Boolean]("PENALTY")
  def * = (date, homeTeam, awayTeam, team, scorer, scoreMinute, ownGoal, penalty)
}