package io.teknek.twitter;

import io.teknek.cassandra.CassandraBatchingOperator;
import io.teknek.cassandra.CassandraOperator;
import io.teknek.daemon.BeLoudOperator;
import io.teknek.daemon.TeknekDaemon;
import io.teknek.feed.FixedFeed;
import io.teknek.plan.FeedDesc;
import io.teknek.plan.OperatorDesc;
import io.teknek.plan.Plan;
import io.teknek.util.MapBuilder;
import io.teknek.zookeeper.EmbeddedZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
 
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EndToEndTest extends EmbeddedZooKeeperServer {

  TeknekDaemon td = null;
  Plan p;

  @Before
  public void setup() {
    Properties props = new Properties();
    props.put(TeknekDaemon.ZK_SERVER_LIST, zookeeperTestServer.getConnectString());
    td = new TeknekDaemon(props);
    td.init();
  }

  
  public static Map<String,Object> getCredentialsOrDie(){
    URL u = Thread.currentThread().getContextClassLoader().getResource("credentials.json");
    File f = new File(u.getFile());
    if (!f.exists()){
      throw new RuntimeException("credentials.json is not found. It must have twitter credentials to run integration tests");
    }
    ObjectMapper om = new ObjectMapper();
    Map<String,Object> result = null;
    try {
      result = om.readValue(f, Map.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }
    
  @Test
  public void hangAround() throws JsonGenerationException, JsonMappingException, IOException {
    Map<String,Object> params = MapBuilder.makeMap(EmitFieldsMatchingPattern.SOURCE_FIELD, "statusAsText",
            EmitFieldsMatchingPattern.REGEX,  "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]" ) ;
    Map<String,Object> cassandraParams = MapBuilder.makeMap(CassandraOperator.KEYSPACE, "stats",
            CassandraOperator.COLUMN_FAMILY, "stats",
            CassandraOperator.HOST_LIST, "localhost:9157", 
            CassandraBatchingOperator.BATCH_SIZE, 1,
            CassandraOperator.PORT, 9157,
            CassandraOperator.INCREMENT, true); 
    p = new Plan().withFeedDesc(new FeedDesc().withFeedClass(TwitterStreamFeed.class.getName()).withProperties(getCredentialsOrDie()));
    p.withRootOperator(new OperatorDesc(new EmitStatusAsTextOperator())
      .withNextOperator(new OperatorDesc(new EmitFieldsMatchingPattern()).withParameters(params)  
        .withNextOperator(new OperatorDesc(new EmitUrlParts())
        .withNextOperator(new OperatorDesc(new CassandraBatchingOperator()).withParameters(cassandraParams))))
    );
    
    p.setName("yell");
    p.setMaxWorkers(1);
    td.applyPlan(p);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @After
  public void shutdown() {
    td.deletePlan(p);
    td.stop();
  }
}