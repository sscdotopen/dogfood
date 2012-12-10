package io.ssc.dogfood.transitionmatrix;

import eu.stratosphere.pact.common.contract.CompilerHints;
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
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.util.FieldSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class CreateTransitionMatrix implements PlanAssembler {

  public Plan getPlan(String... args) {
    int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
    String dataInput = (args.length > 1 ? args[1] : "");
    String output    = (args.length > 2 ? args[2] : "");

    int sizeOfLong = 8;
    int sizeOfDouble = 4;
    int sizeOfInt = 4;

    int averageDegree = 25;
    float nonDanglingVertexRatio = 0.8f;

    FileDataSource source = new FileDataSource(AdjacencyListInputFormat.class, dataInput, "AdjacencyListInput");

    MapContract outDegreeMap = MapContract.builder(NonDanglingVertexMap.class)
        .input(source)
        .name("OutDegree")
        .build();

    CompilerHints outDegreeMapCompilerHints = outDegreeMap.getCompilerHints();
//    outDegreeMapCompilerHints.setAvgBytesPerRecord(sizeOfLong);
//    outDegreeMapCompilerHints.setAvgRecordsEmittedPerStubCall(nonDanglingVertexRatio);
//    outDegreeMapCompilerHints.setUniqueField(new FieldSet(new int[] { Positions.VERTEX_ID }));


    MapContract adjacencyListToEdgesMap = MapContract.builder(AdjacencyListToEdgesMap.class)
        .input(source)
        .name("AdjacencyListToEdges")
        .build();

    CompilerHints adjacencyListToEdgesMapCompilerHints = adjacencyListToEdgesMap.getCompilerHints();
    adjacencyListToEdgesMapCompilerHints.setAvgBytesPerRecord(sizeOfLong + sizeOfInt + averageDegree * sizeOfLong);
    adjacencyListToEdgesMapCompilerHints.setAvgRecordsEmittedPerStubCall(averageDegree);
    adjacencyListToEdgesMapCompilerHints.setUniqueField(
        new FieldSet(new int[] { Positions.VERTEX_ID, Positions.ADJACENT_VERTEX_ID }));


    MatchContract removeDanglingVerticesMatch = MatchContract.builder(RemoveDanglingVerticesMatch.class, PactLong.class,
        Positions.ADJACENT_VERTEX_ID, Positions.VERTEX_ID)
        .input1(adjacencyListToEdgesMap)
        .input2(outDegreeMap)
        .name("RemoveDanglingVerticesMatch")
        .build();

    CompilerHints removeDanglingVerticesMatchCompilerHints = removeDanglingVerticesMatch.getCompilerHints();
    removeDanglingVerticesMatchCompilerHints.setAvgBytesPerRecord(2 * sizeOfLong);
    removeDanglingVerticesMatchCompilerHints.setAvgRecordsEmittedPerStubCall(1);
    removeDanglingVerticesMatchCompilerHints.setUniqueField(new FieldSet(new int[] { Positions.ADJACENT_VERTEX_ID,
        Positions.VERTEX_ID }));


    ReduceContract reducer = new ReduceContract.Builder(TransitionMatrixEntriesReducer.class, PactLong.class,
        Positions.ADJACENT_VERTEX_ID)
        .input(removeDanglingVerticesMatch)
        .name("TransitionMatrixEntries")
        .build();

    CompilerHints reducerCompilerHints = reducer.getCompilerHints();
    reducerCompilerHints.setAvgBytesPerRecord(2 * sizeOfLong + sizeOfDouble);
    reducerCompilerHints.setAvgRecordsEmittedPerStubCall(averageDegree * nonDanglingVertexRatio);
    reducerCompilerHints.setUniqueField(new FieldSet(new int[] { Positions.VERTEX_ID,
        Positions.INCIDENT_VERTEX_ID }));

    FileDataSink out = new FileDataSink(RecordOutputFormat.class, output, reducer, "TransitionProbabilities");
    RecordOutputFormat.configureRecordFormat(out)
        .recordDelimiter('\n')
        .fieldDelimiter('\t')
        .lenient(true)
        .field(PactLong.class, Positions.VERTEX_ID)
        .field(PactLong.class, Positions.INCIDENT_VERTEX_ID)
        .field(PactDouble.class, Positions.TRANSITION_PROBABILITY);

    Plan plan = new Plan(out, "CreateTransitionMatrix");
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

  @StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = 1)
  @StubAnnotation.ConstantFields(fields = { Positions.VERTEX_ID })
  public static class NonDanglingVertexMap extends MapStub {
    @Override
    public void map(PactRecord record, Collector<PactRecord> collector) throws Exception {
      int outDegree = record.getField(Positions.ADJACENT_VERTICES, PactLongArray.class).length();
      if (outDegree > 0) {
        collector.collect(record);
      }
      record.setNull(Positions.ADJACENT_VERTICES);
    }
  }

  @StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = StubAnnotation.OutCardBounds.UNBOUNDED)
  @StubAnnotation.ConstantFieldsExcept(fields = { Positions.ADJACENT_VERTEX_ID, Positions.ADJACENT_VERTICES })
  public static class AdjacencyListToEdgesMap extends MapStub {
    @Override
    public void map(PactRecord record, Collector<PactRecord> collector) throws Exception {
      PactLong adjacentVertex = new PactLong();
      PactLongArray adjacentVertices = record.getField(Positions.ADJACENT_VERTICES, PactLongArray.class);
      for (long adjacentVertexId : adjacentVertices.values()) {
        adjacentVertex.setValue(adjacentVertexId);
        record.setField(Positions.ADJACENT_VERTEX_ID, adjacentVertex);
        collector.collect(record);
      }

      record.setNull(Positions.ADJACENT_VERTICES);
    }
  }

  @StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
  @StubAnnotation.ConstantFieldsFirst(fields = { Positions.VERTEX_ID, Positions.ADJACENT_VERTICES,
      Positions.ADJACENT_VERTEX_ID })
  @StubAnnotation.ConstantFieldsSecond(fields = { Positions.VERTEX_ID, Positions.ADJACENT_VERTICES,
      Positions.ADJACENT_VERTEX_ID })
  public static class RemoveDanglingVerticesMatch extends MatchStub {
    @Override
    public void match(PactRecord edge, PactRecord vertexWithOutDegree, Collector<PactRecord> collector)
        throws Exception {
      collector.collect(edge);
    }
  }

  @StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = StubAnnotation.OutCardBounds.UNBOUNDED)
  @StubAnnotation.ConstantFieldsExcept(fields = { Positions.VERTEX_ID, Positions.INCIDENT_VERTEX_ID,
      Positions.TRANSITION_PROBABILITY })
  public static class TransitionMatrixEntriesReducer extends ReduceStub {
    @Override
    public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector) throws Exception {
      List<Long> incidentVertices = new ArrayList<Long>();
      PactRecord record = null;
      while (records.hasNext()) {
        record = records.next();
        incidentVertices.add(record.getField(Positions.VERTEX_ID, PactLong.class).getValue());
      }
      PactLong targetVertex = record.getField(Positions.ADJACENT_VERTEX_ID, PactLong.class);
      PactLong sourceVertex = new PactLong();
      PactDouble transitionProbability = new PactDouble(1d / (double) incidentVertices.size());
      for (long incidentVertex : incidentVertices) {
        sourceVertex.setValue(incidentVertex);
        record.setField(Positions.VERTEX_ID, targetVertex);
        record.setField(Positions.INCIDENT_VERTEX_ID, sourceVertex);
        record.setField(Positions.TRANSITION_PROBABILITY, transitionProbability);
        collector.collect(record);
      }
    }
  }

}
