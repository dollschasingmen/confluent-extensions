package com.github.dollschasingmen.confluent.extensions.connect.transforms

import java.util.Calendar

import org.apache.kafka.connect.data.{ Schema, SchemaBuilder, Struct }
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }

class InsertRuntimeFieldSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val testInitializationTime: Long = Calendar.getInstance().getTimeInMillis
  val xform = new InsertRuntimeField[SinkRecord]()

  after {
    xform.close()
  }

  "InsertRuntimeField#transform" should "add a runtime given default configuration values" in {
    val prop = new java.util.HashMap[String, AnyRef]
    prop.put("field.start", "startTimestamp")
    xform.configure(prop)

    val originalSchema: Schema = SchemaBuilder.struct
      .name("name").version(1).doc("doc")
      .field("startTimestamp", Schema.INT64_SCHEMA).build

    val originalStruct: Struct = new Struct(originalSchema).put("startTimestamp", testInitializationTime)

    val record = new SinkRecord("test", 0, null, null, originalSchema, originalStruct, 0L)

    // transform record
    val xformedRecord = xform.apply(record)
    val xformedSchema = xformedRecord.valueSchema()

    // assert things that should remain the same
    originalSchema.name() shouldEqual xformedSchema.name()
    originalSchema.version() shouldEqual xformedSchema.version()
    originalSchema.doc() shouldEqual xformedSchema.doc()

    // assert original schema and value still exist
    xformedSchema.field("startTimestamp").schema() shouldEqual Schema.INT64_SCHEMA
    xformedRecord.value().asInstanceOf[Struct].getInt64("startTimestamp").longValue() shouldEqual testInitializationTime

    // assert that we added a runtime with the correct schema
    xformedSchema.field("runtime").schema() shouldEqual Schema.INT64_SCHEMA
    xformedRecord.value().asInstanceOf[Struct].getInt64("runtime").longValue() should be > 0L

  }

  "InsertRuntimeField#transform" should "add a runtime value for specified field names" in {
    val prop = new java.util.HashMap[String, AnyRef]
    prop.put("field.start", "startTimestamp")
    prop.put("field.end", "endTimestamp")
    prop.put("field.runtime", "yoloTime")
    xform.configure(prop)

    val delta = 100L

    val originalSchema: Schema = SchemaBuilder.struct
      .name("name").version(1).doc("doc")
      .field("startTimestamp", Schema.INT64_SCHEMA)
      .field("endTimestamp", Schema.INT64_SCHEMA).build

    val originalStruct: Struct = new Struct(originalSchema)
      .put("startTimestamp", testInitializationTime)
      .put("endTimestamp", testInitializationTime + delta)

    val record = new SinkRecord("test", 0, null, null, originalSchema, originalStruct, 0L)

    // transform record
    val xformedRecord = xform.apply(record)
    val xformedSchema = xformedRecord.valueSchema()

    // assert that we added a runtime with the correct schema
    xformedSchema.field("yoloTime").schema() shouldEqual Schema.INT64_SCHEMA
    xformedRecord.value().asInstanceOf[Struct].getInt64("yoloTime").longValue() shouldEqual delta
  }

}
