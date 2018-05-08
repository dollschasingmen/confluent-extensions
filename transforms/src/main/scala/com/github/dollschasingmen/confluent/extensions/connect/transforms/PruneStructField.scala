package com.github.dollschasingmen.confluent.extensions.connect.transforms

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import com.github.dollschasingmen.confluent.extensions.connect.transforms.util.CopyMethods
import org.apache.kafka.connect.transforms.util.Requirements.requireSinkRecord
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct
import org.apache.kafka.connect.data.{ Field, Schema, Struct }
import scala.collection.JavaConversions._

class PruneStructField[R <: ConnectRecord[R]]
    extends org.apache.kafka.connect.transforms.Transformation[R] with CopyMethods {

  private val PURPOSE = "Prune null values from optional fields from struct records"

  override def config(): ConfigDef = new ConfigDef()
  override def configure(props: java.util.Map[String, _]): Unit = { /* no configuration for this transform */ }
  override def close(): Unit = { /* nothing to do on close */ }

  override def apply(record: R): R = {
    requireSinkRecord(record, PURPOSE)

    val valueStruct = requireStruct(record.value(), PURPOSE)

    if (valueStruct != null) {
      applyToStruct(record, valueStruct)
    } else {
      record
    }
  }

  private def applyToStruct(record: R, valueStruct: Struct): R = {
    pruneStruct(valueStruct) match {
      case Some(p) => record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, p.schema(), p, record.timestamp)
      case None    => record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, valueStruct.schema(), null, record.timestamp)
    }
  }

  private def pruneStruct(struct: Struct): Option[Struct] = {
    val schema = struct.schema()
    val prunedSchema = copyStructSchemaHeadersToSchemaBuilder(struct)

    val prunedFields = schema.fields().filter(f => !isOptionalFieldNull(f, struct))

    if (prunedFields.isEmpty) return None

    prunedFields.foreach { f =>
      f.schema().`type`() match {
        case Schema.Type.STRUCT => pruneStruct(struct.get(f).asInstanceOf[Struct]).foreach(s => prunedSchema.field(f.name, s.schema()))
        case _                  => prunedSchema.field(f.name(), f.schema())
      }
    }

    val prunedStruct = new Struct(prunedSchema.build())

    prunedFields.foreach { f =>
      f.schema().`type`() match {
        case Schema.Type.STRUCT => pruneStruct(struct.get(f).asInstanceOf[Struct]).foreach(s => prunedStruct.put(f.name(), s))
        case _                  => prunedStruct.put(f.name(), struct.get(f))
      }
    }

    Option(prunedStruct)
  }

  private def isOptionalFieldNull(field: Field, struct: Struct): Boolean = {
    val value = struct.get(field)
    val fieldSchema = field.schema()

    if (value == null && fieldSchema.isOptional) true else false
  }

}
