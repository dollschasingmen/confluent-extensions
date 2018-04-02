package com.github.dollschasingmen.confluent.extensions.connect.transforms

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{ SchemaBuilder, Struct, Timestamp }
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
  private val MAX_CACHE_SIZE = 16

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

  private val cache: SchemaEvolvingCache = new SchemaEvolvingCache(schema => {
    val builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)

    for (field <- schema.fields) {
      builder.field(field.name, field.schema)
    }

    builder.field(timestampField, Timestamp.SCHEMA)
    builder.build
  }, MAX_CACHE_SIZE)

  override def config(): ConfigDef = CONFIG_DEF

  override def configure(props: util.Map[String, _]): Unit = {
    val config = new SimpleConfig(CONFIG_DEF, props)
    timestampField = config.getString(ConfigName.TIMESTAMP_FIELD)
    cache.reset()
  }

  override def apply(record: R): R = {
    requireSinkRecord(record, PURPOSE)

    Option(record.valueSchema) match {
      case Some(_) => applyWithSchema(record)
      case None    => applySchemaLess(record)
    }
  }

  override def close(): Unit = cache.reset()

  private def applySchemaLess(record: R): R = {
    val value = requireMap(record.value, PURPOSE)
    val updatedValue = new util.HashMap[String, AnyRef](value)
    updatedValue.put(timestampField, Calendar.getInstance().getTime)
    record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, null, updatedValue, record.timestamp)
  }

  private def applyWithSchema(record: R): R = {
    val value = requireStruct(record.value, PURPOSE)
    val evolvedSchema = cache.getOrElseUpdate(value.schema())
    val updatedValue = new Struct(evolvedSchema)
    updatedValue.put(timestampField, Calendar.getInstance().getTime)
    record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, evolvedSchema, updatedValue, record.timestamp)
  }
}
