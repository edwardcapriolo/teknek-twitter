package io.teknek.twitter;

import io.teknek.collector.Collector;
import io.teknek.model.Tuple;

import org.junit.Assert;
import org.junit.Test;

public class TestEmitUrlParts {

  @Test
  public void simple() throws InterruptedException{
    String url = "http://www.teknek.io/offer";
    EmitUrlParts operator = new EmitUrlParts();
    Collector i = new Collector();
    operator.setCollector(i);
    operator.handleTuple(new Tuple().withField("out", url));
    Assert.assertEquals("io", i.take().getField("out"));
    Assert.assertEquals("io:teknek", i.take().getField("out"));
    Assert.assertEquals("io:teknek:www", i.take().getField("out"));
    Assert.assertEquals("io:teknek:www:/offer", i.take().getField("out"));
    Assert.assertEquals(0, i.size());
  }
  
}
