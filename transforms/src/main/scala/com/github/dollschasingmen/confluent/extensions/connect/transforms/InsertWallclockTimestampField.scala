package com.github.dollschasingmen.confluent.extensions.connect.transforms

import org.apache.kafka.common.config.{ ConfigDef, ConfigException }
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{ Schema, SchemaBuilder, Struct, Timestamp }
import org.apache.kafka.connect.transforms.util.SimpleConfig
import org.apache.kafka.connect.transforms.util.SchemaUtil
import java.util.Calendar
import java.util

import scala.collection.JavaConversions._
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
  private val PURPOSE = "wall clock timestamp field insertion"
  private val MAX_CACHE_SIZE = 16

  private object ConfigName {
    val TIMESTAMP_FIELD = "timestamp.field"
  }

  private var wallClockTsField: Option[String] = None

  private val CONFIG_DEF = new ConfigDef()
    .define(
      ConfigName.TIMESTAMP_FIELD,
      ConfigDef.Type.STRING,
      null,
      ConfigDef.Importance.HIGH,
      "Field name for wall clock timestamp"
    )

  private val cache: SchemaEvolvingCache = new SchemaEvolvingCache(schema => {
    val builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)

    for (field <- schema.fields) {
      builder.field(field.name, field.schema)
    }

    builder.field(wallClockTsField.get, Schema.INT64_SCHEMA)
    builder.build
  }, MAX_CACHE_SIZE)

  override def config(): ConfigDef = CONFIG_DEF

  override def configure(props: util.Map[String, _]): Unit = {
    val config = new SimpleConfig(CONFIG_DEF, props)

    wallClockTsField = Option(config.getString(ConfigName.TIMESTAMP_FIELD))

    if (wallClockTsField.isEmpty) {
      throw new ConfigException(s"No value specified for ${ConfigName.TIMESTAMP_FIELD}")
    }

    println("configured - timestamp field = " + wallClockTsField)

    cache.reset()
    println("configured - timestamp field 2 = " + wallClockTsField)
  }

  override def apply(record: R): R = {
    println("apply - timestamp field = " + wallClockTsField)
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
    updatedValue.put(wallClockTsField.get, new java.lang.Long(Calendar.getInstance().getTimeInMillis))
    record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, null, updatedValue, record.timestamp)
  }

  private def applyWithSchema(record: R): R = {
    val value = requireStruct(record.value, PURPOSE)
    val evolvedSchema = cache.getOrElseUpdate(value.schema())

    val updatedValue = new Struct(evolvedSchema)

    for (field <- value.schema.fields) {
      updatedValue.put(field.name, value.get(field))
    }

    updatedValue.put(wallClockTsField.get, new java.lang.Long(Calendar.getInstance().getTimeInMillis))

    record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, evolvedSchema, updatedValue, record.timestamp)
  }
}
