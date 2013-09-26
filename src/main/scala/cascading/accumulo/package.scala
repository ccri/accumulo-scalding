package cascading

import cascading.scheme.Scheme
import cascading.tap.Tap
import org.apache.accumulo.core.data.{Value, Key}
import org.apache.hadoop.mapred.{RecordWriter, RecordReader, OutputCollector, JobConf}

package object accumulo {
  type GenericRecordReader = RecordReader[_, _]
  type GenericOutputCollector = OutputCollector[_, _]
  type GenericScheme = Scheme[JobConf, GenericRecordReader, GenericOutputCollector, _, _]
  type GenericTap = Tap[_, _, _]
  type KVRecordReader = RecordReader[Key,Value]
  type KVRecordWriter = RecordWriter[Key, Value]
  type KVOutputCollector = OutputCollector[Key,Value]
  type KVTap = Tap[JobConf, KVRecordReader, KVOutputCollector]
  type KVScheme = Scheme[JobConf, KVRecordReader, KVOutputCollector, Array[Any], Array[Any]]

}
