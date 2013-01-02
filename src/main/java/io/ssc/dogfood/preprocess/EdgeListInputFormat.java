package io.ssc.dogfood.preprocess;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

import java.util.regex.Pattern;

public class EdgeListInputFormat extends TextInputFormat {

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

    record.clear();
    record.setField(Positions.VERTEX_ID, vertexId);
    record.setField(Positions.ADJACENT_VERTEX_ID, adjacentVertexId);

    return true;
  }
}