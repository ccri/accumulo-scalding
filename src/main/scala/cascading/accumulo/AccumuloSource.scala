package cascading.accumulo

import cascading.flow.FlowProcess
import cascading.scheme.{SourceCall, SinkCall}
import cascading.tap.Tap
import cascading.tuple.{TupleEntrySchemeCollector, TupleEntryIterator, TupleEntryCollector}
import com.twitter.scalding._
import java.util.UUID
import org.apache.accumulo.core.conf.AccumuloConfiguration
import org.apache.accumulo.core.data.{Value, Key}
import org.apache.accumulo.core.file.FileOperations
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.Progressable

class AccumuloSource(instance: String,
                     tableName: String,
                     outputPath: String)
  extends Source {

  override val hdfsScheme =
    new AccumuloScheme(outputPath).asInstanceOf[GenericScheme]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): GenericTap =
    mode match {
      case hdfsMode @ Hdfs(_, _) => readOrWrite match {
        case Read => new AccumuloTap(tableName, new AccumuloScheme(outputPath))
        case Write => new AccumuloTap(tableName, new AccumuloScheme(outputPath))
      }
      case _ => super.createTap(readOrWrite)(mode)
    }
}

class AccumuloTap(tableName: String, scheme: AccumuloScheme)
  extends Tap[JobConf, KVRecordReader, KVOutputCollector](scheme) {

  val id = UUID.randomUUID().toString
  def getIdentifier: String = id
  def openForRead(fp: FlowProcess[JobConf], rr: KVRecordReader): TupleEntryIterator = ???

  def openForWrite(fp: FlowProcess[JobConf], out: KVOutputCollector): TupleEntryCollector =
    new TupleEntrySchemeCollector[JobConf, KVOutputCollector](fp, scheme, out)

  def createResource(conf: JobConf): Boolean = true

  def deleteResource(conf: JobConf): Boolean = true

  def resourceExists(conf: JobConf): Boolean = true

  def getModifiedTime(conf: JobConf): Long = System.currentTimeMillis()
}

class AccumuloFileOutputFormatDeprecated extends org.apache.hadoop.mapred.FileOutputFormat[Key,Value]  {
  def getRecordWriter(fs: FileSystem, conf: JobConf, s: String, progressable: Progressable): KVRecordWriter = {
    val file = new Path(FileOutputFormat.getWorkOutputPath(conf), FileOutputFormat.getUniqueName(conf, "accingest") + ".rf")
    val skvWriter =
      FileOperations.getInstance().openWriter(
        file.toString(),
        file.getFileSystem(conf),
        conf,
        AccumuloConfiguration.getDefaultConfiguration())

    skvWriter.startDefaultLocalityGroup()

    new KVRecordWriter {
      var hasData = false

      def write(k: Key, v: Value) {
        skvWriter.append(k,v)
        hasData = true
      }

      def close(reporter: Reporter) {
        skvWriter.close()
        if (!hasData) file.getFileSystem(conf).delete(file, false)
      }
    }
  }

}
class AccumuloScheme(outputPath: String) extends KVScheme {
  def sourceConfInit(fp: FlowProcess[JobConf], tap: KVTap, conf: JobConf) {}

  def sinkConfInit(fp: FlowProcess[JobConf], tap: KVTap, conf: JobConf) {
    conf.setOutputFormat(classOf[AccumuloFileOutputFormatDeprecated])
    conf.setOutputKeyClass(classOf[Key])
    conf.setOutputValueClass(classOf[Value])
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
  }

  def source(fp: FlowProcess[JobConf], sourceCall: SourceCall[Array[Any], KVRecordReader]): Boolean = true

  def sink(fp: FlowProcess[JobConf], sinkCall: SinkCall[Array[Any], KVOutputCollector]) {
    val entry = sinkCall.getOutgoingEntry
    val outputCollector = sinkCall.getOutput
    val k = entry.getObject("key").asInstanceOf[Key]
    val v = entry.getObject("value").asInstanceOf[Value]

    outputCollector.collect(k,v)
  }
}
