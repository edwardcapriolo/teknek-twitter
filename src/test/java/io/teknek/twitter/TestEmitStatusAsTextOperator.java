package io.teknek.twitter;

import io.teknek.collector.Collector;
import io.teknek.model.ITuple;
import io.teknek.model.Tuple;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;

import junit.framework.Assert;

import org.junit.Test;

public class TestEmitStatusAsTextOperator {

  @Test
  public void testWithMessage() throws IOException, InterruptedException{
    URL u = Thread.currentThread().getContextClassLoader().getResource("msg.json");
    File f = new File(u.getFile());
    byte [] b = Files.readAllBytes(f.toPath());
    EmitStatusAsTextOperator e = new EmitStatusAsTextOperator();
    Collector i = new Collector();
    e.setCollector(i);
    e.handleTuple(new Tuple().withField("message", new String(b)));
    Assert.assertEquals(1, i.size());
    Tuple tweet = (Tuple) i.peek();
    System.out.println(tweet.getField("statusAsText").toString());
    Assert.assertEquals("La vida", tweet.getField("statusAsText").toString().substring(0,7));
  }
}
