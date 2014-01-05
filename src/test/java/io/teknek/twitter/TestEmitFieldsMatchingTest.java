package io.teknek.twitter;

import junit.framework.Assert;
import io.teknek.collector.Collector;
import io.teknek.model.ITuple;
import io.teknek.model.Tuple;
import io.teknek.util.MapBuilder;

import org.junit.Test;

public class TestEmitFieldsMatchingTest {

  @Test
  public void matchingTest() throws InterruptedException{

    EmitFieldsMatchingPattern e = new EmitFieldsMatchingPattern();
    e.setProperties(    MapBuilder.makeMap(EmitFieldsMatchingPattern.SOURCE_FIELD, "text",
            EmitFieldsMatchingPattern.REGEX,  "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]" ) );
    Collector i = new Collector();
    e.setCollector(i);
    ITuple tuple = new Tuple().withField("text", "my favorite site is http://teknek.io but https://stuff.teknek.io is good to");
    e.handleTuple(tuple);
    ITuple one = i.take();
    Assert.assertEquals(one.getField("out"), "http://teknek.io");
    one = i.take();
    Assert.assertEquals(one.getField("out"), "https://stuff.teknek.io");
  }
}
