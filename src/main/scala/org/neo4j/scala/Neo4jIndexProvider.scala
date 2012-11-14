package org.neo4j.scala

import org.neo4j.graphdb.index.{Index, RelationshipIndex}
import collection.JavaConversions._
import org.neo4j.graphdb.{PropertyContainer, Node}

/**
 * Provides Index access trait
 * @author Christopher Schmidt
 */

trait Neo4jIndexProvider {

  /**
   * type convenience definition
   */
  type IndexCustomConfig = Option[Map[String, String]]

  /**
   * required DatabaseService provided by XXXServiceProvider
   */
  val ds: DatabaseService

  /**
   * has to be overwritten to define Node Index and configuration
   */
  def NodeIndexConfig: List[(String, IndexCustomConfig)] = Nil

  /**
   * has to be overwritten to define Relation Index and configuration
   */
  def RelationIndexConfig: List[(String, IndexCustomConfig)] = Nil

  /** Lazy initializes Indexes for Nodes */
  lazy val getNodeIndexStore: Map[String, Index[Node]] = (for {
      (indexName, indexConfig) <- NodeIndexConfig
      config = indexConfig match {
        case Some(c) => getIndexManager.forNodes(indexName, c)
        case _ => getIndexManager.forNodes(indexName)
      }
    } yield indexName -> config).toMap

  /** Lazy initializes Indexes for Nodes */
  lazy val getRelationIndexStore = (for {
      (indexName, indexConfig) <- RelationIndexConfig
      config = indexConfig match {
        case Some(c) => getIndexManager.forRelationships(indexName, c)
        case _ => getIndexManager.forRelationships(indexName)
      }
    } yield indexName -> config).toMap

  /**
   * returns the index manager
   * @return IndexManager the index manager
   */
  def getIndexManager = ds.gds.index

  /**
   * @return Option[Index[Node]] the created index if available
   */
  def getNodeIndex(name: String) = getNodeIndexStore.get(name)

  /**
   * @return Option[RelationshipIndex] the created index if available
   */
  def getRelationIndex(name: String) = getRelationIndexStore.get(name)

  /**
   * conversion to ease the use of optional configuration
   */
  implicit def mapToOptionMap(t: (String, Map[String, String])) = (t._1, Option(t._2))

  /**
   * wrapper class for subsequent implicit conversion
   */
  class IndexWrapper[T <: PropertyContainer](i: Index[T]) {
    def +=(t: T, k: String, v: AnyRef) = i.add(t, k, v)

    def -=(t: T, k: String, v: AnyRef) = i.remove(t, k, v)

    def -=(t: T, k: String) = i.remove(t, k)

    def -=(t: T) = i.remove(t)
  }

  /**
   * more convenient index adding
   */
  implicit def indexToRichIndex[T <: PropertyContainer](i: Index[T]) = new IndexWrapper[T](i)

}