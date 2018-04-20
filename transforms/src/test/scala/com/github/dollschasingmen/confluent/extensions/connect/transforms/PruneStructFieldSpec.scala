package com.github.dollschasingmen.confluent.extensions.connect.transforms

import org.scalatest.{ FlatSpec, Matchers }
import org.apache.kafka.connect.data.{ Schema, SchemaBuilder, Struct }
import org.apache.kafka.connect.sink.SinkRecord
import scala.collection.JavaConversions._

class PruneStructFieldSpec extends FlatSpec with Matchers {

  val xform = new PruneStructField[SinkRecord]()

  // construct record
  val nestedSchema: Schema = SchemaBuilder.struct
    .name("nested").version(1).doc("doc")
    .field("here", Schema.OPTIONAL_INT64_SCHEMA)
    .field("notHere", Schema.OPTIONAL_INT64_SCHEMA)
    .build

  val nestedStruct: Struct = new Struct(nestedSchema)
    .put("here", 42L)
    .put("notHere", null)

  val emptySchema: Schema = SchemaBuilder.struct
    .name("empty").version(1).doc("doc")
    .field("dark", Schema.OPTIONAL_INT64_SCHEMA)
    .field("void", Schema.OPTIONAL_INT64_SCHEMA)
    .build

  val emptyStruct: Struct = new Struct(emptySchema)
    .put("dark", null)
    .put("void", null)

  val recordSchema: Schema = SchemaBuilder.struct
    .name("record").version(1).doc("doc")
    .field("nested", nestedSchema)
    .field("empty", emptySchema)
    .field("someAlpha", Schema.STRING_SCHEMA)
    .build

  val recordStruct: Struct = new Struct(recordSchema)
    .put("nested", nestedStruct)
    .put("empty", emptyStruct)
    .put("someAlpha", "X")

  val record = new SinkRecord("test", 0, null, null, recordSchema, recordStruct, 0L)

  "PruneStructField#apply" should "prune null optional fields from the map" in {

    // transform record
    println("input record: " + record)

    val xformedRecord = xform.apply(record)
    val xformedSchema = xformedRecord.valueSchema()

    println("xformedRecord: " + xformedRecord)
  }

}
