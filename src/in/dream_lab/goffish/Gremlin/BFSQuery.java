package in.dream_lab.goffish.Gremlin;
import com.thinkaurelius.titan.core.TitanEdge; 
import com.thinkaurelius.titan.core.TitanFactory; 
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.tinkerpop.blueprints.Direction; 
import com.tinkerpop.blueprints.Edge; 
import com.tinkerpop.blueprints.Vertex; 
import com.tinkerpop.gremlin.java.GremlinPipeline; 
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe;
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle; 
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.BaseConfiguration;

public class BFSQuery { 
  
  private TitanGraph titanGraph = null; 
  private BaseConfiguration conf;
  
  public static void main(String args[]) { 
//   GraphDatabase graph = new TitanGraphDatabase(); 
//   graph.createGraphForMassiveLoad(GraphDatabaseBenchmark.TITANDB_PATH); 
//   graph.massiveModeLoading("./data/youtubeEdges.txt"); 
//   graph.shutdownMassiveGraph(); 
    
   BFSQuery titanQuery = new BFSQuery(); 
   titanQuery.bfsQuery("patid",4563712,3);
  } 
   
  
  


  public BFSQuery(StandardTitanGraph titanGraph) { 
   this.titanGraph = titanGraph; 
  } 
  
  public BFSQuery() { 
//      titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("storage.cassandra.keyspace","recreated").set("storage.connection-timeout","5000000").set("storage.setup-wait","2000000").set("index.search.backend","elasticsearch").set("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("cache.db-cache","true").open(); 
    conf = new BaseConfiguration();
    conf.setProperty("storage.backend","cassandra");
    conf.setProperty("storage.cassandra.keyspace","recreated");
    conf.setProperty("storage.connection-timeout","5000000");
    conf.setProperty("storage.setup-wait","2000000");
    conf.setProperty("index.search.backend","elasticsearch");
    conf.setProperty("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15");
    conf.setProperty("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15");
    conf.setProperty("cache.db-cache","true");
    titanGraph =  TitanFactory.open(conf); 
  } 
   
 
  private void TestQuery() {
    // TODO Auto-generated method stub
    
  }
  
  @SuppressWarnings("unused") 
  public void bfsQuery(String key,Object value,final int depth) {

    final HashSet<Object> visitedSet=new HashSet<>();
    
    PipeFunction<Vertex, Boolean> bfsFilterFunction = new PipeFunction<Vertex, Boolean>() {
      public Boolean compute(Vertex i)
      {
          Object patid = i.getProperty("patid");
          if(visitedSet.contains(i.getProperty("patid"))){
              visitedSet.add(patid);
              return true;
          }
          else
              return false;
      }

  };
    
    
    PipeFunction<LoopBundle<Vertex>,Boolean> whileFunction = new PipeFunction<LoopBundle<Vertex>,Boolean>(){

      @Override
      public Boolean compute(LoopBundle<Vertex> bundle) {
       return bundle.getLoops()<= depth && bundle.getObject()!= null;
      }

     
      
    };
    

    PipeFunction<LoopBundle<Vertex>,Boolean> emitFunction = new PipeFunction<LoopBundle<Vertex>,Boolean>(){

      @Override
      public Boolean compute(LoopBundle<Vertex> bundle) {
       return (bundle.getLoops()== depth+1) || (bundle.getLoops()< depth+1 && bundle.getObject().getEdges(Direction.OUT).iterator().hasNext()==false);
      }

     
      
    };
    
    
    GremlinPipeline pipe =new GremlinPipeline(titanGraph).V(key, value).as("x").out().filter(bfsFilterFunction).loop("x", whileFunction,emitFunction).path();
    
    System.out.println(pipe.count());

  }
  
  
  public void shortestPath(){
    final Vertex v1 = titanGraph.getVertex(2);
    final Vertex v2 = titanGraph.getVertex(6);
    final Set<Vertex> x = new HashSet<>(Collections.singleton(v1));

    final GremlinPipeline<Object, List> pipe = new GremlinPipeline<>(v1).as("x")
            .both().except(x).store(x).loop("x", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                @Override
                public Boolean compute(LoopPipe.LoopBundle<Vertex> bundle) {
                    return !x.contains(v2);
                }
            }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
                @Override
                public Boolean compute(LoopPipe.LoopBundle<Vertex> bundle) {
                    return bundle.getObject() == v2;
                }
            }).path();
    for (final List path : pipe) {
        System.out.println(path);
    }
    
  }
  
  
//  public void bfsQuery(String key, Object val){
//    
//    final Set<Vertex> x = new HashSet<>();
//
//    final GremlinPipeline<Object, List> pipe = new GremlinPipeline<>(titanGraph).V(key, val).as("x")
//            .store(x).out().loop("x", new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
//                @Override
//                public Boolean compute(LoopPipe.LoopBundle<Vertex> bundle) {
//                    Vertex v =bundle.getObject();
//                    v = Gremlin
//                    
//                }
//            }, new PipeFunction<LoopPipe.LoopBundle<Vertex>, Boolean>() {
//                @Override
//                public Boolean compute(LoopPipe.LoopBundle<Vertex> bundle) {
//                    return bundle.getObject() == v2;
//                }
//            }).path();
//    for (final List path : pipe) {
//        System.out.println(path);
//    }
//    
//    
//  }
   


   
 }



/*AZURE TITAN CONFIGURATION
conf = new BaseConfiguration();
conf.setProperty("storage.backend","cassandra");
conf.setProperty("storage.cassandra.keyspace","recreated");
conf.setProperty("storage.connection-timeout","5000000");
conf.setProperty("storage.setup-wait","2000000");
conf.setProperty("index.search.backend","elasticsearch");
conf.setProperty("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15");
conf.setProperty("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15");
conf.setProperty("cache.db-cache","true");
g = TitanFactory.open(conf);

*/


/*
 * titanGraph = TitanFactory.build().set("storage.backend","cassandra").set("storage.cassandra.keyspace","recreated").set("storage.connection-timeout","5000000").set("storage.setup-wait","2000000").set("index.search.backend","elasticsearch").set("storage.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("index.search.hostname","10.0.0.12,10.0.0.13,10.0.0.14,10.0.0.15").set("cache.db-cache","true").open(); 
 */
