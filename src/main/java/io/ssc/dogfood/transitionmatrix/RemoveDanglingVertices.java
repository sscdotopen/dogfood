package io.ssc.dogfood.transitionmatrix;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.Iterator;
import java.util.regex.Pattern;

public class RemoveDanglingVertices implements PlanAssembler {

  public Plan getPlan(String... args) {
    int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
    String dataInput = (args.length > 1 ? args[1] : "");
    String output    = (args.length > 2 ? args[2] : "");

    FileDataSource source = new FileDataSource(EdgeListInputFormat.class, dataInput, "EdgeListInput");

    MapContract hasOutlinkMap = MapContract.builder(HasOutlinkMap.class)
        .input(source)
        .name("HasOutlinkMap")
        .build();

    ReduceContract hasOutlinkReduce = ReduceContract.builder(HasOutLinkReduce.class, PactLong.class, 0)
        .input(hasOutlinkMap)
        .name("HasOutlinkReduce")
        .build();

    MatchContract nonDanglingOnlyMatch = MatchContract.builder(NonDanglingOnlyMatch.class, PactLong.class, 1, 0)
        .input1(source)
        .input2(hasOutlinkReduce)
        .name("NonDanglingOnlyMatch")
        .build();

    FileDataSink out = new FileDataSink(RecordOutputFormat.class, output, nonDanglingOnlyMatch, "EdgeListOutput");
    RecordOutputFormat.configureRecordFormat(out)
        .recordDelimiter('\n')
        .fieldDelimiter('\t')
        .lenient(true)
        .field(PactLong.class, 0)
        .field(PactLong.class, 1);

    Plan plan = new Plan(out, "RemoveOneLayerOfDanglingVertices");
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

  public static class HasOutlinkMap extends MapStub {

    PactRecord result = new PactRecord();

    @Override
    public void map(PactRecord record, Collector<PactRecord> collector) throws Exception {
      result.setField(0, record.getField(0, PactLong.class));
      collector.collect(result);
    }
  }

  @ReduceContract.Combinable
  public static class HasOutLinkReduce extends ReduceStub {

    private PactRecord result = new PactRecord();

    @Override
    public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector) throws Exception {
      PactRecord first = records.next();
      result.setField(0, first.getField(0, PactLong.class));

      while (records.hasNext()) {
        records.next();
      }
      collector.collect(result);
    }

  }

  public static class NonDanglingOnlyMatch extends MatchStub {
    @Override
    public void match(PactRecord record, PactRecord record1, Collector<PactRecord> collector) throws Exception {
      collector.collect(record);
    }
  }



}
