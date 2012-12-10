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
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.runtime.iterative.playing.scopedpagerank.SequentialAccessSparseRowVector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class RowPartitionedTransitionMatrix implements PlanAssembler {

  public Plan getPlan(String... args) {
    int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
    String dataInput = (args.length > 1 ? args[1] : "");
    String output    = (args.length > 2 ? args[2] : "");

    FileDataSource source = new FileDataSource(EntryListInputFormat.class, dataInput, "EdgeListInput");

    MapContract edgeMap = MapContract.builder(IdentityMap.class)
        .input(source)
        .name("EntryMap")
        .build();

    ReduceContract reducer = new ReduceContract.Builder(ToRowsReducer.class, PactLong.class,
        Positions.VERTEX_ID)
        .input(edgeMap)
        .name("ToRowsReducer")
        .build();

    FileDataSink out = new FileDataSink(RecordOutputFormat.class, output, reducer, "AdjacencyList");
    RecordOutputFormat.configureRecordFormat(out)
        .recordDelimiter('\n')
        .fieldDelimiter(' ')
        .lenient(true)
        .field(PactLong.class, Positions.VERTEX_ID)
        .field(SequentialAccessSparseRowVector.class, Positions.ROW);

    Plan plan = new Plan(out, "RowPartitionedTransitionMatrix");
    plan.setDefaultParallelism(noSubTasks);
    return plan;
  }

  public static class EntryListInputFormat extends TextInputFormat {

    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public boolean readRecord(PactRecord record, byte[] bytes, int offset, int numBytes) {
      String str = new String(bytes, offset, numBytes).trim();
      String[] parts = SEPARATOR.split(str);

      if ("".equals(parts[0])) {
        return false;
      }

      PactLong vertexId = new PactLong(Long.parseLong(parts[0]));
      PactLong adjacentVertexId = new PactLong(Long.parseLong(parts[1]));
      PactDouble prob = new PactDouble(Double.parseDouble(parts[2]));

      record.clear();
      record.setField(Positions.VERTEX_ID, vertexId);
      record.setField(Positions.ADJACENT_VERTEX_ID, adjacentVertexId);
      record.setField(Positions.TRANSITION_PROBABILITY, prob);

      return true;
    }
  }


  @StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
  @StubAnnotation.ConstantFields(fields = { Positions.VERTEX_ID, Positions.ADJACENT_VERTEX_ID })
  public static class IdentityMap extends MapStub {
    @Override
    public void map(PactRecord record, Collector<PactRecord> collector) throws Exception {
      collector.collect(record);
    }
  }


  @StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
  @StubAnnotation.ConstantFields(fields = { Positions.VERTEX_ID })
  public static class ToRowsReducer extends ReduceStub {

    private PactRecord record;

    @Override
    public void open(Configuration parameters) throws Exception {
      record = new PactRecord();
    }

    @Override
    public void close() throws Exception {
      record = null;
    }

    @Override
    public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector) throws Exception {

      List<Long> neighborsList = new ArrayList<Long>(100);
      List<Double> valuesList = new ArrayList<Double>(100);

      boolean first = true;

      while (records.hasNext()) {
        PactRecord nextRecord = records.next();

        if (first) {
          record.setField(Positions.VERTEX_ID, nextRecord.getField(Positions.VERTEX_ID, PactLong.class));
          first = false;
        }

        long neighborId = nextRecord.getField(Positions.ADJACENT_VERTEX_ID, PactLong.class).getValue();
        double value = nextRecord.getField(Positions.TRANSITION_PROBABILITY, PactDouble.class).getValue();
        neighborsList.add(neighborId);
        valuesList.add(value);
      }

      long[] indexes = new long[neighborsList.size()];
      int n = 0;
      for (long index : neighborsList) {
        indexes[n++] = index;
      }

      double[] values = new double[indexes.length];
      n = 0;
      for (double value : valuesList) {
        values[n++] = value;
      }

      record.setField(Positions.ROW, new SequentialAccessSparseRowVector(indexes, values));

      collector.collect(record);
    }
  }
}