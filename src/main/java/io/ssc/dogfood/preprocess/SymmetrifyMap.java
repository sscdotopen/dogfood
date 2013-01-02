package io.ssc.dogfood.preprocess;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public class SymmetrifyMap extends MapStub {

  private PactRecord record = new PactRecord();

  @Override
  public void map(PactRecord input, Collector<PactRecord> collector) throws Exception {

    record.setField(Positions.VERTEX_ID, input.getField(Positions.ADJACENT_VERTEX_ID, PactLong.class));
    record.setField(Positions.ADJACENT_VERTEX_ID, input.getField(Positions.VERTEX_ID, PactLong.class));
    collector.collect(record);
    collector.collect(input);
  }
}