package com.github.dollschasingmen.confluent.extensions.connect.transforms
import java.util
import java.util.Calendar

import org.apache.kafka.connect.data.{ Schema, SchemaBuilder, Struct }
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }

class InsertWallclockTimestampFieldSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val testInitializationTime: Long = Calendar.getInstance().getTimeInMillis
  val xform = new InsertWallclockTimestampField[SinkRecord]()

  after {
    xform.close()
  }

  "InsertWallclockTimestampField#configure" should "throw exception if no timestamp.field is specified" in {
    val prop = new util.HashMap[String, AnyRef]
    an[org.apache.kafka.common.config.ConfigException] should be thrownBy xform.configure(prop)
  }

  "InsertWallclockTimestampField#transform" should "add an wall clock timestamp value for specified field name" in {
    // configure
    val prop = new util.HashMap[String, AnyRef]
    prop.put("timestamp.field", "updated_at")
    xform.configure(prop)

    // make record
    val originalSchema: Schema = SchemaBuilder.struct
      .name("name").version(1).doc("doc")
      .field("magic", Schema.OPTIONAL_INT64_SCHEMA).build

    val originalStruct: Struct = new Struct(originalSchema).put("magic", 42L)

    val record = new SinkRecord("test", 0, null, null, originalSchema, originalStruct, 0L)

    // transform record
    val xformedRecord = xform.apply(record)
    val xformedSchema = xformedRecord.valueSchema()

    // assert things that should remain the same
    originalSchema.name() shouldEqual xformedSchema.name()
    originalSchema.version() shouldEqual xformedSchema.version()
    originalSchema.doc() shouldEqual xformedSchema.doc()

    // assert original schema and value still exist
    xformedSchema.field("magic").schema() shouldEqual Schema.OPTIONAL_INT64_SCHEMA
    xformedRecord.value().asInstanceOf[Struct].getInt64("magic").longValue() shouldEqual 42L

    // assert that we added a timestamp with the correct schema
    xformedSchema.field("updated_at").schema() shouldEqual Schema.INT64_SCHEMA
    xformedRecord.value().asInstanceOf[Struct].getInt64("updated_at").longValue() should be > testInitializationTime
  }
}
