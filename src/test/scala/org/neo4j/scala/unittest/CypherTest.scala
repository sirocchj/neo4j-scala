package org.neo4j.scala.unittest

import org.specs2.mutable.SpecificationWithJUnit
import org.neo4j.scala._
import org.neo4j.scala.Test_Matrix
import sys.ShutdownHookThread

class CypherSpec extends SpecificationWithJUnit with Neo4jWrapper with EmbeddedGraphDatabaseServiceProvider with Cypher {

  val store = "/tmp/temp-neo-CypherTest"

  final val nodes = Map("Neo" -> "Hacker",
    "Morpheus" -> "Hacker",
    "Trinity" -> "Hacker",
    "Cypher" -> "Hacker",
    "Agent Smith" -> "Program",
    "The Architect" -> "Whatever")

  val nodeMap = withTx {
    implicit neo =>
      val nodeMap = for ((name, prof) <- nodes) yield (name, createNode(Test_Matrix(name, prof)))

      getReferenceNode --> "ROOT" --> nodeMap("Neo")

      nodeMap("Neo") --> "KNOWS" --> nodeMap("Trinity")
      nodeMap("Neo") --> "KNOWS" --> nodeMap("Morpheus") --> "KNOWS" --> nodeMap("Trinity")
      nodeMap("Morpheus") --> "KNOWS" --> nodeMap("Cypher") --> "KNOWS" --> nodeMap("Agent Smith")
      nodeMap("Agent Smith") --> "CODED_BY" --> nodeMap("The Architect")
      nodeMap
  }

  val startNodes = nodeMap("Neo") :: nodeMap("Morpheus") :: nodeMap("Trinity") :: Nil

  "Cypher Trait" should {

    "be able to prepare query and launch it multiple times" in {

      val query = "start n=node({id}) return n, n.name"
      val executionPlan = query.prepare

      val params1 = Map("id" -> nodeMap("Neo").getId)
      val node1 = executionPlan.execute(params1).asCC[Test_Matrix]("n")
      val params2 = Map("id" -> List(nodeMap("Neo"), nodeMap("Morpheus")).map(_.getId))
      val nodes = executionPlan.execute(params2).asCC[Test_Matrix]("n").toList
      node1.next().name must be_==("Neo")
      nodes.size must be_==(2)
      nodes must contain(Test_Matrix("Neo", "Hacker"), Test_Matrix("Morpheus", "Hacker"))

      success
    }

    "be able to execute query" in {

      val query = "start n=node(" + nodeMap("Neo").getId + ") return n, n.name"

      val typedResult = query.execute.asCC[Test_Matrix]("n")

      typedResult.next().name must be_==("Neo")

      success
    }

  }

}