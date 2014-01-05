package io.teknek.twitter;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;

public class EmitFieldsMatchingPattern extends Operator {

  public static final String SOURCE_FIELD = "source.field";
  public static final String OUT_FIELD = "out.field";
  public static final String REGEX = "regex.expression";
  private String sourceField;
  private String outField;
  private Pattern pattern ;
  
  //String regex = "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]"

  @Override
  public void setProperties(Map<String, Object> properties) {
    super.setProperties(properties);
    sourceField = (String) properties.get(SOURCE_FIELD);
    outField = (String) properties.get(OUT_FIELD);
    if (outField == null){
      outField = "out";
    }
    pattern = Pattern.compile((String) properties.get(REGEX), Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
  }

  @Override
  public void handleTuple(ITuple tuple) {
    Matcher m = pattern.matcher((String) tuple.getField(sourceField));
    while (m.find()) {
      ITuple out = new Tuple();
      out.setField(outField, m.group());
      collector.emit(out);
    }
  }
 
}
