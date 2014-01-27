package io.teknek.twitter;

import java.net.URI;
import java.util.Arrays;
import java.util.Stack;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;

public class EmitUrlParts extends Operator {

  @Override
  public void handleTuple(ITuple tuple) {
    String url = (String) tuple.getField("out");
    URI i = URI.create(url);
    String domain = i.getHost();
    String path = i.getPath();
    String[] parts = domain.split("\\.");
    Stack<String> s = new Stack<String>();
    s.add(path);
    s.addAll(Arrays.asList(parts));
    StringBuilder sb = new StringBuilder();
    for (int j = 0; j <= parts.length; j++) {
      sb.append(s.pop());
      collector.emit(new Tuple().withField("out", sb.toString()));
      sb.append(":");
    }
  }

}
