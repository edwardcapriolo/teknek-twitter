package io.teknek.twitter;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;

import junit.framework.Assert;
import io.teknek.collector.Collector;
import io.teknek.model.Tuple;

import org.junit.Test;

import twitter4j.Status;

public class TestEmitStatusOperator {

  @Test
  public void aTest(){
    String del = "{message={\"delete\":{\"status\":{\"id\":419701742134378496,\"user_id\":2147894047,\"id_str\":\"419701742134378496\",\"user_id_str\":\"2147894047\"}}}}";
    EmitStatusOperator e = new EmitStatusOperator();
    Collector i = new Collector();
    e.setCollector(i);
    Tuple t = new Tuple();
    t.withField("message", del);
    e.handleTuple(t);
    Assert.assertEquals(0, i.size());
  }
  
  @Test
  public void testWithMessage() throws IOException, InterruptedException{
    URL u = Thread.currentThread().getContextClassLoader().getResource("msg.json");
    File f = new File(u.getFile());
    byte [] b = Files.readAllBytes(f.toPath());
    EmitStatusOperator e = new EmitStatusOperator();
    Collector i = new Collector();
    e.setCollector(i);
    Tuple t = new Tuple();
    t.withField("message", new String(b));
    e.handleTuple(t);
    Assert.assertEquals(1, i.size());
    Tuple tweet = (Tuple) i.peek();
    Status s = (Status) tweet.getField("status");
    Assert.assertEquals("La vida", s.getText().substring(0,7));
  }
  
}
