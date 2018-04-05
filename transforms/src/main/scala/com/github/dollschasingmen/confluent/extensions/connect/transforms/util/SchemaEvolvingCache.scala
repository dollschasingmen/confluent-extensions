package com.github.dollschasingmen.confluent.extensions.connect.transforms.util

import org.apache.kafka.common.cache.{ LRUCache, SynchronizedCache }
import org.apache.kafka.connect.data.Schema

class SchemaEvolvingCache(evolveSchema: Schema => Schema, maxSize: Int = 16) {

  private var cache = new SynchronizedCache[Schema, Schema](new LRUCache[Schema, Schema](maxSize))

  def getOrElseUpdate(schema: Schema): Schema = {
    Option(cache.get(schema)) match {
      case Some(cachedEvolvedSchema) => cachedEvolvedSchema
      case None =>
        val evolvedSchema = evolveSchema(schema)
        cache.put(schema, evolvedSchema)
        evolvedSchema
    }
  }

  def reset(): Unit = cache = new SynchronizedCache[Schema, Schema](new LRUCache[Schema, Schema](maxSize))
}
