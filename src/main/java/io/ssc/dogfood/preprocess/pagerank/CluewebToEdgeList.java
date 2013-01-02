package io.ssc.dogfood.preprocess.pagerank;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import io.ssc.dogfood.preprocess.PactLongArray;
import io.ssc.dogfood.preprocess.Positions;

public class CluewebToEdgeList implements PlanAssembler {

  @Override
  public Plan getPlan(String... args) {
    int noSubTasks   = args.length > 0 ? Integer.parseInt(args[0]) : 1;
    String dataInput = args.length > 1 ? args[1] : "";
    String output    = args.length > 2 ? args[2] : "";

    FileDataSource source = new FileDataSource(AdjacencyListInputFormat.class, dataInput, "CluewebInput");

    MapContract toEdges = MapContract.builder(ToEdgesMap.class)
          .input(source)
          .name("ToEdgeListMap")
          .build();

    FileDataSink out = new FileDataSink(RecordOutputFormat.class, output, toEdges, "EdgeList");
    RecordOutputFormat.configureRecordFormat(out)
        .recordDelimiter('\n')
        .fieldDelimiter(' ')
        .lenient(true)
        .field(PactLong.class, Positions.VERTEX_ID)
        .field(PactLong.class, Positions.ADJACENT_VERTEX_ID);

    Plan plan = new Plan(out, "CluewebToEdgeList");
    plan.setDefaultParallelism(noSubTasks);
    return plan;
  }

  public static class ToEdgesMap extends MapStub {

    PactRecord result = new PactRecord();
    PactLong adjacentNeighbor = new PactLong();

    @Override
    public void map(PactRecord record, Collector<PactRecord> collector) throws Exception {
      result.setField(Positions.VERTEX_ID, record.getField(0, PactLong.class));

      long[] neighborIDs = record.getField(1, PactLongArray.class).values();
      for (long neighborID : neighborIDs) {
        adjacentNeighbor.setValue(neighborID);
        result.setField(Positions.ADJACENT_VERTEX_ID, adjacentNeighbor);
        collector.collect(result);
      }
    }
  }


}
