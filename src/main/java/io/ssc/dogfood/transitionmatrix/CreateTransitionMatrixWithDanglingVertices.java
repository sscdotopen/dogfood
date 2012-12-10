package io.ssc.dogfood.transitionmatrix;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.regex.Pattern;

public class CreateTransitionMatrixWithDanglingVertices implements PlanAssembler {

  public Plan getPlan(String... args) {
    int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
    String dataInput = (args.length > 1 ? args[1] : "");
    String output    = (args.length > 2 ? args[2] : "");

    FileDataSource source = new FileDataSource(AdjacencyListInputFormat.class, dataInput, "AdjacencyListInput");

    MapContract entries = MapContract.builder(TransitionMatrixEntryMap.class)
        .input(source)
        .name("TransitionMatrixEntries")
        .build();

    FileDataSink out = new FileDataSink(RecordOutputFormat.class, output,  entries, "TransitionProbabilities");
    RecordOutputFormat.configureRecordFormat(out)
        .recordDelimiter('\n')
        .fieldDelimiter('\t')
        .lenient(true)
        .field(PactLong.class, Positions.VERTEX_ID)
        .field(PactLong.class, Positions.INCIDENT_VERTEX_ID)
        .field(PactDouble.class, Positions.TRANSITION_PROBABILITY);

    Plan plan = new Plan(out, "CreateTransitionMatrixWithDanglingVertices");
    plan.setDefaultParallelism(noSubTasks);
    return plan;
  }

  public static class AdjacencyListInputFormat extends TextInputFormat {

    private static final Pattern SEPARATOR = Pattern.compile(" ");

    @Override
    public boolean readRecord(PactRecord record, byte[] bytes, int offset, int numBytes) {
      String str = new String(bytes, offset, numBytes).trim();
      String[] parts = SEPARATOR.split(str);

      if ("".equals(parts[0])) {
        return false;
      }

      PactLong vertexId = new PactLong(Long.parseLong(parts[0]));
      long[] adjacentVertices = new long[parts.length - 1];
      for (int n = 1; n < parts.length; n++) {
        adjacentVertices[n - 1] = Long.parseLong(parts[n]);
      }

      record.clear();
      record.setField(Positions.VERTEX_ID, vertexId);
      record.setField(Positions.ADJACENT_VERTICES, new PactLongArray(adjacentVertices));

      return true;
    }
  }

  public static class TransitionMatrixEntryMap extends MapStub {

    private PactRecord result;

    @Override
    public void open(Configuration parameters) throws Exception {
      result = new PactRecord();
    }

    @Override
    public void map(PactRecord record, Collector<PactRecord> collector) throws Exception {

      long[] adjacentVertices = record.getField(Positions.ADJACENT_VERTICES, PactLongArray.class).values();

      if (adjacentVertices.length == 0) {
        return;
      }

      result.setField(Positions.INCIDENT_VERTEX_ID, record.getField(Positions.VERTEX_ID, PactLong.class));
      result.setField(Positions.TRANSITION_PROBABILITY, new PactDouble(1d / adjacentVertices.length));

      PactLong vertex = new PactLong();
      for (long vertexId : adjacentVertices) {
        vertex.setValue(vertexId);
        result.setField(Positions.VERTEX_ID, vertex);
        collector.collect(result);
      }
    }
  }

}
