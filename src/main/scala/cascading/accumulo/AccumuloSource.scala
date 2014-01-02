package cascading.accumulo

import cascading.flow.FlowProcess
import cascading.scheme.{SourceCall, SinkCall}
import cascading.tap.Tap
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator
import cascading.tuple._
import com.twitter.scalding._
import java.io.{DataOutput, DataInput}
import java.util.UUID
import org.apache.accumulo.core.client.mapreduce.InputFormatBase.RangeInputSplit
import org.apache.accumulo.core.client.mapreduce.{InputFormatBase, AccumuloInputFormat}
import org.apache.accumulo.core.conf.AccumuloConfiguration
import org.apache.accumulo.core.data.{Value, Key}
import org.apache.accumulo.core.file.FileOperations
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.Progressable

class AccumuloSource(instance: String,
                     tableName: String,
                     outputPath: String)
  extends Source with Mappable[(Key,Value)] {

  val hdfsScheme = new AccumuloScheme(outputPath).asInstanceOf[GenericScheme]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): GenericTap =
    mode match {
      case hdfsMode @ Hdfs(_, _) => readOrWrite match {
        case Read => new AccumuloTap(tableName, new AccumuloScheme(outputPath))
        case Write => new AccumuloTap(tableName, new AccumuloScheme(outputPath))
      }
      case _ => throw new NotImplementedError()
    }


  def converter[U >: (Key, Value)]: TupleConverter[U] = new TupleConverter[U] {
    def arity: Int = 2
    def apply(te: TupleEntry): (Key, Value) = (te.getObject(0).asInstanceOf[Key], te.getObject(1).asInstanceOf[Value])
  }
}

class AccumuloTap(tableName: String, scheme: AccumuloScheme)
  extends Tap[JobConf, KVRecordReader, KVOutputCollector](scheme) {

  val id = UUID.randomUUID().toString
  def getIdentifier: String = id
  def openForRead(fp: FlowProcess[JobConf], rr: KVRecordReader): TupleEntryIterator =
    new HadoopTupleEntrySchemeIterator(fp, this, rr)

  def openForWrite(fp: FlowProcess[JobConf], out: KVOutputCollector): TupleEntryCollector =
    new TupleEntrySchemeCollector[JobConf, KVOutputCollector](fp, scheme, out)

  def createResource(conf: JobConf): Boolean = true

  def deleteResource(conf: JobConf): Boolean = true

  def resourceExists(conf: JobConf): Boolean = true

  def getModifiedTime(conf: JobConf): Long = System.currentTimeMillis()
}



class AccumuloScheme(outputPath: String) extends KVScheme(new Fields("key","value")) {
  def sourceConfInit(fp: FlowProcess[JobConf], tap: KVTap, conf: JobConf) {
    InputFormatBase.setInputInfo(conf, "root", "secret".getBytes(), "frsmall", new Authorizations())
    InputFormatBase.setZooKeeperInstance(conf, "acloud", "ant:2181,art:2181,stripebranch:2181")
    conf.setInputFormat(classOf[AccumuloInputFormatDeprecated])
  }

  def sinkConfInit(fp: FlowProcess[JobConf], tap: KVTap, conf: JobConf) {
    conf.setOutputFormat(classOf[AccumuloFileOutputFormatDeprecated])
    conf.setOutputKeyClass(classOf[Key])
    conf.setOutputValueClass(classOf[Value])
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
  }

  def source(fp: FlowProcess[JobConf], sourceCall: SourceCall[Array[Any], KVRecordReader]): Boolean = {
    val k = sourceCall.getContext.apply(0)
    val v = sourceCall.getContext.apply(1)

    val hasNext = sourceCall.getInput.next(k.asInstanceOf[Key], v.asInstanceOf[Value])

    if(!hasNext) false
    else {
      val res = new Tuple()
      res.add(k)
      res.add(v)
      sourceCall.getIncomingEntry.setTuple(res)
      true
    }
  }

  def sink(fp: FlowProcess[JobConf], sinkCall: SinkCall[Array[Any], KVOutputCollector]) {
    val entry = sinkCall.getOutgoingEntry
    val outputCollector = sinkCall.getOutput
    val k = entry.getObject("key").asInstanceOf[Key]
    val v = entry.getObject("value").asInstanceOf[Value]

    outputCollector.collect(k,v)
  }

  override def sourcePrepare(flowProcess: FlowProcess[JobConf], sourceCall: SourceCall[Array[Any], KVRecordReader]) {
    sourceCall.setContext(Array(sourceCall.getInput.createKey(), sourceCall.getInput.createValue()))
  }

  override def sourceCleanup(flowProcess: FlowProcess[JobConf], sourceCall: SourceCall[Array[Any], KVRecordReader]) {
    sourceCall.setContext(null)
  }
}

class DelegatingSplitInfo extends InputSplit {
  var is: RangeInputSplit = new RangeInputSplit()
  def this(inputSplit: RangeInputSplit) {
    this()
    is = inputSplit
  }

  def getLength: Long = is.getLength

  def getLocations: Array[String] = is.getLocations

  def write(p1: DataOutput) {
    is.write(p1)
  }

  def readFields(p1: DataInput) {
    is.readFields(p1)
  }
}

class AccumuloInputFormatDeprecated extends org.apache.hadoop.mapred.InputFormat[Key,Value]  {

  import collection.JavaConversions._
  import org.apache.hadoop.{ mapreduce => mr }

  val inputFormat = new AccumuloInputFormat()

  def getSplits(p1: JobConf, p2: Int): Array[InputSplit] = {
    val newRanges = inputFormat.getSplits(new mr.task.JobContextImpl(p1, new JobID()))
    newRanges.map { is => new DelegatingSplitInfo(is.asInstanceOf[RangeInputSplit]) }.toArray
  }

  def getRecordReader(p1: InputSplit, p2: JobConf, p3: Reporter): RecordReader[Key, Value] =

    new RecordReader[Key,Value] {
      val taskAttemptContext = new mr.task.TaskAttemptContextImpl(p2, new mr.TaskAttemptID())
      val delegate = inputFormat.createRecordReader(null, taskAttemptContext)
      delegate.initialize(p1.asInstanceOf[DelegatingSplitInfo].is, taskAttemptContext)

      def close() {
        delegate.close()
      }

      def next(p1: Key, p2: Value): Boolean = {
        val ret = delegate.nextKeyValue()
        if(!ret) false
        else {
          p1.set(delegate.getCurrentKey)
          p2.set(delegate.getCurrentValue.get())
          true
        }
      }

      def createKey(): Key = new Key()

      def createValue(): Value = new Value()

      def getPos: Long = 0

      def getProgress: Float = delegate.getProgress
    }
}

class AccumuloFileOutputFormatDeprecated extends org.apache.hadoop.mapred.FileOutputFormat[Key,Value]  {
  def getRecordWriter(fs: FileSystem, conf: JobConf, s: String, progressable: Progressable): KVRecordWriter = {
    val file = new Path(FileOutputFormat.getWorkOutputPath(conf), FileOutputFormat.getUniqueName(conf, "accingest") + ".rf")
    val skvWriter =
      FileOperations.getInstance().openWriter(
        file.toString,
        file.getFileSystem(conf),
        conf,
        AccumuloConfiguration.getDefaultConfiguration)

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
