package com.github.dollschasingmen.confluent.extensions.connect.transforms

import org.apache.kafka.common.cache.Cache
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{ Schema, SchemaBuilder, Struct, Timestamp }
import org.apache.kafka.connect.transforms.util.SimpleConfig
import org.apache.kafka.connect.transforms.util.SchemaUtil
import java.util.Calendar
import java.util

import org.apache.kafka.connect.transforms.util.Requirements.requireMap
import org.apache.kafka.connect.transforms.util.Requirements.requireSinkRecord
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct

import scala.collection.JavaConversions._

/**
 * "Insert the wall clock time the connect processes the record with a configurable field name.
 *
 * @tparam R
 */
class InsertWallclockTimestampField[R <: ConnectRecord[R]] extends org.apache.kafka.connect.transforms.Transformation[R] {
  val DEFAULT_TIMESTAMP_FIELD = "updatedAt"
  private val PURPOSE = "wall clock timestamp field insertion"

  private object ConfigName {
    val TIMESTAMP_FIELD = "timestamp.field"
  }

  private var timestampField = DEFAULT_TIMESTAMP_FIELD

  private val CONFIG_DEF = new ConfigDef()
    .define(
      ConfigName.TIMESTAMP_FIELD,
      ConfigDef.Type.STRING,
      DEFAULT_TIMESTAMP_FIELD,
      ConfigDef.Importance.HIGH,
      "Field name for wall clock timestamp"
    )

  private var schemaUpdateCache: Cache[Schema, Schema] = new SynchronizedCache[Schema, Schema](new LRUCache[Schema, Schema](16))

  override def config(): ConfigDef = CONFIG_DEF

  override def configure(props: util.Map[String, _]): Unit = {
    val config = new SimpleConfig(CONFIG_DEF, props)
    timestampField = config.getString(ConfigName.TIMESTAMP_FIELD)
    resetCache()
  }

  override def apply(record: R): R = {
    requireSinkRecord(record, PURPOSE)

    Option(record.valueSchema) match {
      case Some(_) => applyWithSchema(record)
      case None    => applySchemaLess(record)
    }
  }

  override def close(): Unit = {
    resetCache()
  }

  private def applySchemaLess(record: R): R = {
    val value = requireMap(record.value, PURPOSE)
    val updatedValue = new util.HashMap[String, AnyRef](value)
    updatedValue.put(timestampField, Calendar.getInstance().getTime)
    record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, null, updatedValue, record.timestamp)
  }

  private def applyWithSchema(record: R): R = {
    val value = requireStruct(record.value, PURPOSE)

    val updatedSchema = Option(schemaUpdateCache.get(value.schema)) match {
      case Some(schema) => schema
      case None =>
        val updatedSchema = makeUpdatedSchema(value.schema)
        schemaUpdateCache.put(value.schema, updatedSchema)
        updatedSchema
    }

    val updatedValue = new Struct(updatedSchema)
    updatedValue.put(timestampField, Calendar.getInstance().getTime)
    record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, updatedSchema, updatedValue, record.timestamp)
  }

  private def makeUpdatedSchema(schema: Schema) = {
    val builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)

    for (field <- schema.fields) {
      builder.field(field.name, field.schema)
    }

    builder.field(timestampField, Timestamp.SCHEMA)
    builder.build
  }

  private def resetCache(): Unit = {
    schemaUpdateCache = new SynchronizedCache[Schema, Schema](new LRUCache[Schema, Schema](16))
  }
}
