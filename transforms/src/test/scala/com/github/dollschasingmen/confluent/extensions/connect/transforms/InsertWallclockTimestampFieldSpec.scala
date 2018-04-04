package com.github.dollschasingmen.confluent.extensions.connect.transforms
import java.util
import java.util.Calendar

import org.apache.kafka.connect.data.{ Schema, SchemaBuilder, Struct }
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }

class InsertWallclockTimestampFieldSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val testInitializationTime = Calendar.getInstance().getTimeInMillis
  def xform = new InsertWallclockTimestampField[SinkRecord]()

  after {
    xform.close()
  }

  def emptyProp(): util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]

  def propWithTimestampField(fieldName: String): util.Map[String, AnyRef] = {
    val prop = emptyProp()
    prop.put("timestamp.field", fieldName)
    prop
  }

  val originalSchema: Schema = SchemaBuilder.struct
    .name("name").version(1).doc("doc")
    .field("magic", Schema.OPTIONAL_INT64_SCHEMA).build

  val originalStruct: Struct = new Struct(originalSchema).put("magic", 42L)

  "InsertWallclockTimestampField transform" should "add a updatedAt" in {
    xform.configure(emptyProp())
    val record = new SinkRecord("test", 0, null, null, originalSchema, originalStruct, 0L)
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
    xformedSchema.field("updatedAt").schema() shouldEqual Schema.INT64_SCHEMA
    xformedRecord.value().asInstanceOf[Struct].getInt64("updatedAt").longValue() should be > testInitializationTime
  }

}
