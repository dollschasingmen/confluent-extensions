package com.github.dollschasingmen.confluent.extensions.connect.transforms

import org.apache.kafka.common.config.{ ConfigDef, ConfigException }
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.transforms.util.SimpleConfig
import java.util._

import scala.collection.JavaConversions._
import com.github.dollschasingmen.confluent.extensions.connect.transforms.util._
import org.apache.kafka.connect.transforms.util.Requirements.requireMap
import org.apache.kafka.connect.transforms.util.Requirements.requireSinkRecord
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct

import scala.collection.mutable

class InsertRuntimeField[R <: ConnectRecord[R]]
    extends org.apache.kafka.connect.transforms.Transformation[R] with CopyMethods {

  private val PURPOSE = "Given a start time and end time field, calculate run time for record"
  private val DEFAULT_RUNTIME_FIELD = "runtime"

  private object ConfigName {
    val START_TIME = "field.start"
    val END_TIME = "field.end"
    val RUNTIME = "field.runtime"
  }

  private var configHolder: mutable.Map[String, String] = mutable.Map.empty

  private val CONFIG_DEF = new ConfigDef()
    .define(
      ConfigName.START_TIME,
      ConfigDef.Type.STRING,
      null,
      ConfigDef.Importance.HIGH,
      "Field name for the start time"
    )
    .define(
      ConfigName.END_TIME,
      ConfigDef.Type.STRING,
      null,
      ConfigDef.Importance.MEDIUM,
      "Field name for the end time"
    )
    .define(
      ConfigName.RUNTIME,
      ConfigDef.Type.STRING,
      DEFAULT_RUNTIME_FIELD,
      ConfigDef.Importance.MEDIUM,
      "Field name for the runtime value"
    )

  private val cache: SchemaEvolvingCache = new SchemaEvolvingCache(schema => {
    val builder = copySchema(schema)
    builder.field(configHolder(ConfigName.RUNTIME), Schema.INT64_SCHEMA)
    builder.build
  })

  override def config(): ConfigDef = CONFIG_DEF

  override def configure(props: java.util.Map[String, _]): Unit = {
    val config = new SimpleConfig(CONFIG_DEF, props)

    config.values().keySet().foreach(k => {
      configHolder.put(k, config.getString(k))
    })

    if (configHolder.fetch(ConfigName.START_TIME).isEmpty) {
      throw new ConfigException(s"No value specified for ${ConfigName.START_TIME}")
    }

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

    val updatedValue = new java.util.HashMap[String, AnyRef](value)
    updatedValue.put(
      configHolder(ConfigName.RUNTIME),
      calculateRuntime(MapRecordValue(value))
    )

    record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, null, updatedValue, record.timestamp)
  }

  private def applyWithSchema(record: R): R = {
    val value = requireStruct(record.value, PURPOSE)

    val evolvedSchema = cache.getOrElseUpdate(value.schema())

    val updatedValue = copyStructWithSchema(value, evolvedSchema)
    updatedValue.put(
      configHolder(ConfigName.RUNTIME),
      calculateRuntime(StructRecordValue(value))
    )

    record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, evolvedSchema, updatedValue, record.timestamp)
  }

  def calculateRuntime(value: RecordValue[String, AnyRef]): java.lang.Long = {
    val startTime = value.get(
      configHolder(ConfigName.START_TIME)
    ).asInstanceOf[java.lang.Long]

    val endTime = configHolder.fetch(ConfigName.END_TIME) match {
      case Some(endTimeField) => value.get(endTimeField).asInstanceOf[java.lang.Long]
      case _                  => new java.lang.Long(Calendar.getInstance().getTimeInMillis)
    }

    endTime - startTime
  }

  implicit class MapImprovments(m: mutable.Map[String, String]) {
    def fetch(k: String): Option[String] = m.get(k) match {
      case Some(v) => Option(v)
      case None    => None
    }
  }
}
