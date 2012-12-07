package org.neo4j.scala

import collection.JavaConversions._
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProviderNewImpl
import org.neo4j.unsafe.batchinsert.BatchInserterIndex
import sys.ShutdownHookThread

/**
 * provides Index access trait
 * class must mixin a trait that provides an instance of class BatchInserter
 * i.g. BatchGraphDatabaseServiceProvider
 */
trait Neo4jBatchIndexProvider {

  this: BatchGraphDatabaseServiceProvider =>

  lazy val batchInserterIndexProvider = {
    val batchInserterIndexProvider = new LuceneBatchInserterIndexProviderNewImpl(batchInserter)
    ShutdownHookThread {
      batchInserterIndexProvider.shutdown()
    }
    batchInserterIndexProvider
  }

  def indexForNodes(indexName: String, customConfiguration: Map[String, String]) =
    batchInserterIndexProvider.nodeIndex(indexName, customConfiguration)

  def indexForRelationships(indexName: String, customConfiguration: Map[String, String]) =
    batchInserterIndexProvider.relationshipIndex(indexName, customConfiguration)

  /**
   * wrapper class for subsequent implicit conversion
   */
  class RichIndex(i: BatchInserterIndex) {

    def +=(entityId: Long, properties: Map[String, AnyRef]) {
      i.add(entityId, properties)
    }

  }

  /**
   * more convenient index adding
   */
  implicit def indexToRichIndex(i: BatchInserterIndex) = new RichIndex(i)

}