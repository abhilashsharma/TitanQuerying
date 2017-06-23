package in.dream_lab.goffish.Gremlin;
import java.util.HashSet;

import org.apache.commons.configuration.BaseConfiguration;

import com.thinkaurelius.titan.core.TitanEdge; 
import com.thinkaurelius.titan.core.TitanFactory; 
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.tinkerpop.blueprints.Direction; 
import com.tinkerpop.blueprints.Edge; 
import com.tinkerpop.blueprints.Vertex; 
import com.tinkerpop.gremlin.java.GremlinPipeline; 
import com.tinkerpop.pipes.PipeFunction; 
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle; 

public class BFSQuery { 
  
  private TitanGraph titanGraph = null; 
  private BaseConfiguration conf;
  private HashSet<Integer> vistedNodes;
  public static void main(String args[]) { 
//   GraphDatabase graph = new TitanGraphDatabase(); 
//   graph.createGraphForMassiveLoad(GraphDatabaseBenchmark.TITANDB_PATH); 
//   graph.massiveModeLoading("./data/youtubeEdges.txt"); 
//   graph.shutdownMassiveGraph(); 
    
   BFSQuery titanQuery = new BFSQuery(); 
   titanQuery.TestQuery();
  } 
   
  private void TestQuery() {
    // TODO Auto-generated method stub
    
  }

  public BFSQuery(TitanGraph titanGraph) { 
   this.titanGraph = titanGraph; 
  } 
   
  public BFSQuery() { 
        conf = new BaseConfiguration();
        conf.setProperty("storage.backend","cassandra");
        conf.setProperty("storage.cassandra.keyspace","recreated");
        conf.setProperty("storage.connection-timeout","5000000");
        conf.setProperty("storage.setup-wait","2000000");
        conf.setProperty("index.search.backend","elasticsearch");
        conf.setProperty("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15");
        conf.setProperty("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15");
        conf.setProperty("cache.db-cache","true");
        titanGraph = TitanFactory.open(conf) ;
  } 
   
 
  @SuppressWarnings("unused") 
  public void findNeighborsOfAllNodes() { 
   for (Vertex v : titanGraph.getVertices()) { 
    for (Vertex vv : v.getVertices(Direction.BOTH, "similar")) { 
    } 
   } 
  } 
   
   

   
 }


/*AZURE TITAN CONFIGURATION
conf = new BaseConfiguration()
conf.setProperty("storage.backend","cassandra")
conf.setProperty("storage.cassandra.keyspace","recreated")
conf.setProperty("storage.connection-timeout","5000000")
conf.setProperty("storage.setup-wait","2000000")
conf.setProperty("index.search.backend","elasticsearch")
conf.setProperty("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15")
conf.setProperty("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15")
conf.setProperty("cache.db-cache","true")
g = TitanFactory.open(conf)
*/