package io.ssc.dogfood.preprocess.pagerank;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import io.ssc.dogfood.preprocess.PactLongArray;

import java.util.regex.Pattern;

public class AdjacencyListInputFormat extends TextInputFormat {

  private static final Pattern SEPARATOR = Pattern.compile("[, \t]");

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
    String str = new String(bytes, offset, numBytes);
    String[] parts = SEPARATOR.split(str);

    target.clear();
    target.addField(new PactLong(Long.parseLong(parts[0])));

    int numEntries = parts.length - 1;
    long[] ids = new long[numEntries];

    for (int n = 0; n < numEntries; n++) {
      ids[n] = Long.parseLong(parts[n + 1]);
    }

    target.addField(new PactLongArray(ids));

    return true;
  }
}