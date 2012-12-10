package io.ssc.dogfood.transitionmatrix;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.Iterator;
import java.util.regex.Pattern;

public class SymmetrifyEdgeList implements PlanAssembler {

  public Plan getPlan(String... args) {
    int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
    String dataInput = (args.length > 1 ? args[1] : "");
    String output = (args.length > 2 ? args[2] : "");

    FileDataSource source = new FileDataSource(EdgeListInputFormat.class, dataInput, "EdgeListInput");

    MapContract symmetryMap = MapContract.builder(SymmetrifyMap.class)
        .input(source)
        .name("SymmetrifyMap")
        .build();

    ReduceContract reducer = new ReduceContract.Builder(ToEdgeListReducer.class, PactLong.class, 0)
        .input(symmetryMap)
        .name("ToEdgeListReducer")
        .build();

    FileDataSink out = new FileDataSink(RecordOutputFormat.class, output, reducer, "SymmetricEdgeListInput");
    RecordOutputFormat.configureRecordFormat(out)
        .recordDelimiter('\n')
        .fieldDelimiter(' ')
        .lenient(true)
        .field(PactLong.class, 0)
        .field(PactLong.class, 1);

    Plan plan = new Plan(out, "Symmetrify (EdgeList)");
    plan.setDefaultParallelism(noSubTasks);
    return plan;
  }

  public static class EdgeListInputFormat extends TextInputFormat {

    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public boolean readRecord(PactRecord record, byte[] bytes, int offset, int numBytes) {
      String str = new String(bytes, offset, numBytes).trim();
      String[] parts = SEPARATOR.split(str);

      if (parts.length != 2 || "".equals(parts[0]) || "".equals(parts[1])) {
        return false;
      }

      PactLong vertexID = new PactLong(Long.parseLong(parts[0]));
      PactLong adjacentVertexID = new PactLong(Long.parseLong(parts[1]));

      record.clear();
      record.setField(0, vertexID);
      record.setField(1, adjacentVertexID);

      return true;
    }
  }


  @StubAnnotation.OutCardBounds(lowerBound = 2, upperBound = 2)
  public static class SymmetrifyMap extends MapStub {

    @Override
    public void map(PactRecord record, Collector<PactRecord> out) throws Exception {

      long from = record.getField(0, PactLong.class).getValue();
      long to = record.getField(1, PactLong.class).getValue();

      PactRecord switched = new PactRecord();
      switched.setField(0, new PactLong(to));
      switched.setField(1, new PactLong(from));
      out.collect(record);
      out.collect(switched);
    }
  }


  @StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
  public static class ToEdgeListReducer extends ReduceStub {

    private PactRecord record;

    @Override
    public void open(Configuration parameters) throws Exception {
      record = new PactRecord();
    }

    @Override
    public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector) throws Exception {

      PactRecord first = records.next();
      record.setField(0, first.getField(0, PactLong.class));
      record.setField(1, first.getField(1, PactLong.class));
      collector.collect(record);

      while (records.hasNext()) {
        PactRecord next = records.next();
        record.setField(1, next.getField(1, PactLong.class));
        collector.collect(record);
      }
    }
  }
}
