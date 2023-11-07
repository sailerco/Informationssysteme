package Abgabe1

import slick.jdbc.H2Profile.api.{Table, Tag, _}

import java.sql.Date

class Results(tag: Tag) extends Table[(Date, String, String, Int, Int, String, String, String, Boolean)](tag, "RESULT_FILE") {
  def date = column[Date]("DATE")

  def homeTeam = column[String]("HOME_TEAM")

  def awayTeam = column[String]("AWAY_TEAM")

  def homeScore = column[Int]("HOME_SCORE")

  def awayScore = column[Int]("AWAY_SCORE")

  def tournament = column[String]("TOURNAMENT")

  def city = column[String]("CITY")

  def country = column[String]("COUNTRY")

  def neutral = column[Boolean]("NEUTRAL")

  def * = (date, homeTeam, awayTeam, homeScore, awayScore, tournament, city, country, neutral)
}
