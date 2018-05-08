package com.github.dollschasingmen.confluent.extensions.connect.transforms.util

import org.apache.kafka.connect.data.Schema
import scala.collection.JavaConversions._

object HelpfulImplicits {

  implicit class SchemaHelpers(s: Schema) {
    def prettyPrint: String = "schema type = " + s.`type`() + ", schema fields = " + s.fields().map(_.name)
  }

}
