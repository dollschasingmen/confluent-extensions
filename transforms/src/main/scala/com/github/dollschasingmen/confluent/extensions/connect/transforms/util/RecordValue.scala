package com.github.dollschasingmen.confluent.extensions.connect.transforms.util

trait RecordValue[K, V] {
  def get(k: K): V
  def put(k: K, v: V): Unit
}

class MapRecordValue(delegate: java.util.Map[String, AnyRef]) extends RecordValue[String, AnyRef] {
  def get(k: String): AnyRef = delegate.get(k)
  def put(k: String, v: AnyRef): Unit = delegate.put(k, v)
}

object MapRecordValue {
  def apply(delegate: java.util.Map[String, AnyRef]): MapRecordValue = new MapRecordValue(delegate)
}

class StructRecordValue(delegate: org.apache.kafka.connect.data.Struct) extends RecordValue[String, AnyRef] {
  def get(k: String): AnyRef = delegate.get(k)
  def put(k: String, v: AnyRef): Unit = delegate.put(k, v)
}

object StructRecordValue {
  def apply(delegate: org.apache.kafka.connect.data.Struct): StructRecordValue = new StructRecordValue(delegate)
}