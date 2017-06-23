package in.dream_lab.goffish.Gremlin;

import org.apache.commons.configuration.BaseConfiguration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;

public class ConfigTest {

  public static void main(String args[])
  {
      BaseConfiguration conf = new BaseConfiguration();
      conf.setProperty("storage.backend","cassandra");
      conf.setProperty("storage.cassandra.keyspace","recreated");
      conf.setProperty("storage.connection-timeout","5000000");
      conf.setProperty("storage.setup-wait","2000000");
      conf.setProperty("index.search.backend","elasticsearch");
      conf.setProperty("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15");
      conf.setProperty("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15");
      conf.setProperty("cache.db-cache","true");

      TitanGraph titanGraph = TitanFactory.open(conf);

//      Vertex rash = titanGraph.addVertex(null);
//      rash.setProperty("userId", 1);
//      rash.setProperty("username", "rash");
//      rash.setProperty("firstName", "Rahul");
//      rash.setProperty("lastName", "Chaudhary");
//      rash.setProperty("birthday", 101);
//
//      Vertex honey = titanGraph.addVertex(null);
//      honey.setProperty("userId", 2);
//      honey.setProperty("username", "honey");
//      honey.setProperty("firstName", "Honey");
//      honey.setProperty("lastName", "Anant");
//      honey.setProperty("birthday", 201);
//
//      Edge frnd = titanGraph.addEdge(null, rash, honey, "FRIEND");
//      frnd.setProperty("since", 2011);
//
//      titanGraph.commit();
//
//      Iterable<Vertex> results = rash.query().labels("FRIEND").has("since", 2011).vertices();
//
//      for(Vertex result : results)
//      {
//          System.out.println("Id: " + result.getProperty("userId"));
//          System.out.println("Username: " + result.getProperty("username"));
//          System.out.println("Name: " + result.getProperty("firstName") + " " + result.getProperty("lastName"));
//      }
  }
}
