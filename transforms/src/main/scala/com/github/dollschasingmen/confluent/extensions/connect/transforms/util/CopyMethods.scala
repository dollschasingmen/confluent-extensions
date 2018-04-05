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

}
