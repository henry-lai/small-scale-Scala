import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import scala.io.Source

object AssignmentPart1 extends App {

  //Define case classes so CountActor knows what format method should be in
  case class StartCounting(s1: String, s2: String, actorRef: ActorRef)
  case class CountSecond(s: String, m: Map[Char, Int])
  case class Combine(m1: Map[Char, Int], m2: Map[Char, Int])

  class CountActor extends Actor {
    def receive: Receive = {
      //Processes 1st half of string into a map. Sends map and 2nd half of string
      case StartCounting(s1, s2, actor2) =>

        val dup: Map[Char, Int] = s1.groupBy(_.toChar).map { p => (p._1, p._2.length) }
        actor2 ! CountSecond(s2, dup)

      //Processes 2nd half of string into a map. Sends map, 1st half of string to Combine()
      case CountSecond(s, dup1) =>

        val dup2: Map[Char, Int] = s.groupBy(_.toChar).map { p => (p._1, p._2.length) }
        sender() ! Combine(dup1, dup2)

      //Combines both strings together into a map and prints it out.
      case Combine(m1, m2) =>

        val list = m1.toList ++ m2.toList
        val merged = list.groupBy(_._1).map { case (k, v) => k -> v.map(_._2).sum }
        println(merged)

      //Wildcard for any other cases
      case _ =>
        println("Something went wrong, try again")
    }
  }

  //Store content from txt file into a String
  val str: String = Source.fromFile("bleak-house.txt").getLines().mkString.trim()

  //Split string in 2 parts
  val str1: String = str.substring(0, str.length() / 2)
  val str2: String = str.substring(str.length() / 2)

  //Create the actor system and actors, and send a message to CountActor with a method
  val system = ActorSystem("count")
  val actor1 = system.actorOf(Props[CountActor], name = "count-actor1")
  val actor2 = system.actorOf(Props[CountActor], name = "count-actor2")
  actor1 ! StartCounting(str1, str2, actor2)

  system.terminate()
}
