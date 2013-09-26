accumulo-scalding
=================

Cascading bindings for enabling Accumulo as a source/sink for Cascading/Scalding

Notional example usage:

```scala
class TestScaldingJob(args : Args) extends Job(args) {
  lazy val out = new AccumuloSource(args("instance"), args("table"), args("outputDir"))

  TextLine(input).flatMap('line -> List('key,'value)) { line: String =>
    line.split(",").groupBy(_).map { case (k, v) =>
        (new Key(new Text(k)), new Value(ByteBuffer.allocate(8).put(v.length)))
  }.groupBy('key) { _.sortBy('value).reducers(32) }.write(out)
}
```
