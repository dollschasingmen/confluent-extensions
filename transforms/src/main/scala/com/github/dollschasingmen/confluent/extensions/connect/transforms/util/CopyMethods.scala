package com.github.dollschasingmen.confluent.extensions.connect.transforms.util

import org.apache.kafka.connect.data.{ Schema, SchemaBuilder, Struct }
import org.apache.kafka.connect.transforms.util.SchemaUtil

import scala.collection.JavaConversions._

trait CopyMethods {

  def copySchema(schema: Schema): SchemaBuilder = {
    val builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)

    for (field <- schema.fields) {
      builder.field(field.name, field.schema)
    }

    builder
  }

  def copyStructWithSchema(toCopy: Struct, retSchema: Schema): Struct = {
    val retValue = new Struct(retSchema)

    for (field <- toCopy.schema.fields) {
      retValue.put(field.name, toCopy.get(field))
    }

    retValue
  }

  def copyStruct(toCopy: Struct): Struct = {
    val builder = SchemaUtil.copySchemaBasics(toCopy.schema, SchemaBuilder.struct)
    val copy = new Struct(builder.build())

    for (field <- toCopy.schema.fields) {
      copy.put(field.name, toCopy.get(field))
    }

    copy
  }

  def copyStructSchemaHeadersToSchemaBuilder(struct: Struct): SchemaBuilder = {
    val schema = struct.schema()

    val name = schema.name()
    val version = schema.version()
    val doc = schema.doc()

    SchemaBuilder.struct.name(name).version(version).doc(doc)
  }

}
