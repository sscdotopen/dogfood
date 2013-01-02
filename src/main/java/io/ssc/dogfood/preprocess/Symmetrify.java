package io.ssc.dogfood.preprocess;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.type.base.PactLong;


public class Symmetrify implements PlanAssembler {

  public Plan getPlan(String... args) {
    int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
    String dataInput = (args.length > 1 ? args[1] : "");
    String output    = (args.length > 2 ? args[2] : "");

    FileDataSource source = new FileDataSource(EdgeListInputFormat.class, dataInput, "EdgeListInput");

    MapContract edgeMap = MapContract.builder(SymmetrifyMap.class)
        .input(source)
        .build();

    FileDataSink out = new FileDataSink(RecordOutputFormat.class, output, edgeMap, "SymmetricEdges");
    RecordOutputFormat.configureRecordFormat(out)
        .recordDelimiter('\n')
        .fieldDelimiter(' ')
        .lenient(true)
        .field(PactLong.class, Positions.VERTEX_ID)
        .field(PactLong.class, Positions.ADJACENT_VERTEX_ID);

    Plan plan = new Plan(out);
    plan.setDefaultParallelism(noSubTasks);
    return plan;
  }



}

