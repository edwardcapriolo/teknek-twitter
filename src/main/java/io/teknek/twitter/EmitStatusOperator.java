package io.teknek.twitter;

import java.io.IOException;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.internal.json.z_T4JInternalJSONImplFactory;
import twitter4j.internal.org.json.JSONException;
import twitter4j.internal.org.json.JSONObject;
import twitter4j.json.JSONObjectType;
import twitter4j.json.JSONObjectType.Type;
import io.teknek.model.ITuple;
import io.teknek.model.Operator;
import io.teknek.model.Tuple;

public class EmitStatusOperator extends Operator {

  private final z_T4JInternalJSONImplFactory factory;
  
  public EmitStatusOperator(){
    this.factory = new z_T4JInternalJSONImplFactory(new ConfigurationBuilder().build());
  }
  
  @Override
  public void handleTuple(ITuple t) {
    String msg = (String) t.getField("message");
    JSONObject json = null;
    try {
      json = new JSONObject(msg);
    } catch (JSONException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Status result = processMessage(json);
    if (result != null){
      ITuple tup = new Tuple();
      tup.setField("status", result);
      collector.emit(tup);
    }
  }
  
  public  Status processMessage(JSONObject json)  {
    JSONObjectType.Type type = JSONObjectType.determine(json);
    if (type.equals(Type.STATUS)){
        Status status = null;
        try {
          status = factory.createStatus(json);
        } catch (TwitterException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        return status;
    } else {
      return null;
    }
  }
}
