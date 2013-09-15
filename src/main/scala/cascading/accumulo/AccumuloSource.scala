package cascading.accumulo

import cascading.flow.FlowProcess
import cascading.scheme.{SourceCall, SinkCall, Scheme}
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
    new AccumuloScheme(outputPath).asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    mode match {
      case hdfsMode @ Hdfs(_, _) => readOrWrite match {
        case Read => new AccumuloTap(tableName, new AccumuloScheme(outputPath))
        case Write => new AccumuloTap(tableName, new AccumuloScheme(outputPath))
      }
      case _ => super.createTap(readOrWrite)(mode)
    }
}

class AccumuloTap(tableName: String, scheme: AccumuloScheme)
  extends Tap[JobConf, RecordReader[Key,Value], OutputCollector[Key,Value]](scheme) {

  val id = UUID.randomUUID().toString
  def getIdentifier: String = id
  def openForRead(fp: FlowProcess[JobConf], rr: RecordReader[Key,Value]): TupleEntryIterator = ???

  def openForWrite(fp: FlowProcess[JobConf], out: OutputCollector[Key, Value]): TupleEntryCollector = 
    new TupleEntrySchemeCollector[JobConf, OutputCollector[Key,Value]](fp, scheme, out)

  def createResource(conf: JobConf): Boolean = true

  def deleteResource(conf: JobConf): Boolean = true

  def resourceExists(conf: JobConf): Boolean = true

  def getModifiedTime(conf: JobConf): Long = System.currentTimeMillis()
}

class AccumuloFileOutputFormatDeprecated extends org.apache.hadoop.mapred.FileOutputFormat[Key,Value]  {
  def getRecordWriter(fs: FileSystem, conf: JobConf, s: String, progressable: Progressable): RecordWriter[Key, Value] = {
    val file = new Path(FileOutputFormat.getWorkOutputPath(conf), FileOutputFormat.getUniqueName(conf, "accingest") + ".rf")
    val skvWriter =
      FileOperations.getInstance().openWriter(
        file.toString(),
        file.getFileSystem(conf),
        conf,
        AccumuloConfiguration.getDefaultConfiguration())

    skvWriter.startDefaultLocalityGroup()

    new RecordWriter[Key,Value] {
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
class AccumuloScheme(outputPath: String) extends Scheme[JobConf, RecordReader[Key,Value], OutputCollector[Key,Value], Array[Any], Array[Any]] {
  def sourceConfInit(fp: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[Key,Value], OutputCollector[Key,Value]], conf: JobConf) {}

  def sinkConfInit(fp: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[Key,Value], OutputCollector[Key,Value]], conf: JobConf) {
    conf.setOutputFormat(classOf[AccumuloFileOutputFormatDeprecated])
    conf.setOutputKeyClass(classOf[Key])
    conf.setOutputValueClass(classOf[Value])
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
  }

  def source(fp: FlowProcess[JobConf], sourceCall: SourceCall[Array[Any], RecordReader[Key,Value]]): Boolean = true

  def sink(fp: FlowProcess[JobConf], sinkCall: SinkCall[Array[Any], OutputCollector[Key,Value]]) {
    val entry = sinkCall.getOutgoingEntry
    val outputCollector = sinkCall.getOutput
    val k = entry.getObject("key").asInstanceOf[Key]
    val v = entry.getObject("value").asInstanceOf[Value]

    outputCollector.collect(k,v)
  }
}
