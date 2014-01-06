package io.teknek.twitter;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;

/**
 * The goal of this operator is to extract all tokens matching a regex pattern from a field of a
 * tuple that is of the string type.
 * 
 * @author edward
 * 
 */
public class EmitFieldsMatchingPattern extends Operator {

  /**
   * The configuration source field of a tuple to run extraction against
   */
  public static final String SOURCE_FIELD = "source.field";

  /**
   * The configuration field populated in the output tuple.
   */
  public static final String OUT_FIELD = "out.field";

  /**
   * The configuration field to match against using regex
   */
  public static final String REGEX = "regex.expression";

  /**
   * The configuration field which specifies the name of the out field. Defaults to out.
   */
  public static final String DEFAULT_OUT_FIELD = "out";

  private String sourceField;

  private String outField;

  private Pattern pattern;

  /**
   * A handy regex to match urls
   */
  public static final String URL_REGEX = "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";

  @Override
  public void setProperties(Map<String, Object> properties) {
    super.setProperties(properties);
    sourceField = (String) properties.get(SOURCE_FIELD);
    outField = (String) properties.get(OUT_FIELD);
    if (outField == null) {
      outField = DEFAULT_OUT_FIELD;
    }
    pattern = Pattern.compile((String) properties.get(REGEX), Pattern.CASE_INSENSITIVE
            | Pattern.MULTILINE | Pattern.DOTALL);
  }

  @Override
  /**
   * Given an input tuple tuple take the input field and run a regex pattern matcher against it. Output each match as a separate 
   * tuple.
   */
  public void handleTuple(ITuple tuple) {
    Matcher m = pattern.matcher((String) tuple.getField(sourceField));
    while (m.find()) {
      ITuple out = new Tuple();
      out.setField(outField, m.group());
      collector.emit(out);
    }
  }

}
