package io.teknek.twitter;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import io.teknek.feed.FeedPartition;
import io.teknek.model.ITuple;
import io.teknek.model.Tuple;
import io.teknek.twitter.TwitterStreamFeed;
import io.teknek.util.MapBuilder;

import org.junit.Ignore;
import org.junit.Test;

public class TestTwitterStreamFeed {

  //@Ignore
  @Test
  public void letsGo(){
    TwitterStreamFeed sf = new TwitterStreamFeed(EndToEndTest.getCredentialsOrDie());
    List<FeedPartition> parts = sf.getFeedPartitions();
    parts.get(0).initialize();
    ITuple it = new Tuple();
    boolean res = parts.get(0).next(it);
    System.out.println(it);
    Assert.assertEquals(true, res);

    Assert.assertNotNull(it.getField("message"));
    res = parts.get(0).next(it);
    System.out.println(it.getField("message"));
  }
}
