 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.util._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import java.io._

object Betweenness {  
  	
		val vprog = { (id: VertexId, attr: Double, msg: Double) => math.min(attr,msg) }
    
    val sendMessage = { (triplet: EdgeTriplet[Double, Int]) =>
		var iter:Iterator[(VertexId, Double)] = Iterator.empty
		val isSrcMarked = triplet.srcAttr != Double.PositiveInfinity
		val isDstMarked = triplet.dstAttr != Double.PositiveInfinity
		if(!(isSrcMarked && isDstMarked)){
		   	if(isSrcMarked){
				iter = Iterator((triplet.dstId,triplet.srcAttr+1))
	  		}else{
				iter = Iterator((triplet.srcId,triplet.dstAttr+1))
	   		}
		}
		iter
   	}
    
    val reduceMessage = { (a: Double, b: Double) => math.min(a,b) }
  
   def main(args: Array[String]){  //start of main
  
    val conf = new SparkConf().setAppName("CF").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
     var  inputFile = sparkContext.textFile(args(0))
        val header = inputFile.first()
    inputFile = inputFile.filter(row => row != header)
        val inputRDD = inputFile.map(line=>line.split(',')).map(line=>(line(0).toInt, line(1).toInt))
        val req = inputRDD.map(x => (x._1 -> x._2)).groupByKey.collect().toMap
        val usersTot = req.size
        
         val users: RDD[(VertexId,Int)] = inputRDD.groupByKey.map(x=>(x._1.toLong,x._1))
         
    val partitionCount = 10
   val  partitionStrategy = PartitionStrategy.fromString("EdgePartition2D")
      var nodesList = ListBuffer[Int]()
         var edgesList =  ListBuffer[Edge[Int]]()
         var creditEdge = scala.collection.mutable.HashMap[(VertexId,VertexId), (Double)]()
        
      for ( i <- 1 to usersTot){
        for (j <- 2 to usersTot){
          if(i < j) {
           if( req(i).toSet.intersect(req(j).toSet).size >=3) {
                nodesList += i
             nodesList += j  //bug fix
              edgesList += Edge(i.toLong, j.toLong, 0)
            creditEdge.put((i,j),0.0)
             
           }
          }
           
        }
      }
        val edges: RDD[Edge[Int]]  =  sparkContext.parallelize(edgesList)   
        val graphNodes = nodesList.distinct
              val nodes: RDD[(VertexId,Int)] = sparkContext.parallelize(graphNodes).map(x=>(x.toLong,x))
        
     val nodeTot = graphNodes.size
        val graph = Graph(nodes, edges)

	val initialMessage = Double.PositiveInfinity		
		val maxIterations = Int.MaxValue
		val activeEdgeDirection = EdgeDirection.Either	
    var rootVertex: VertexId = 0L
    
     var adjNeighbours = graph.collectNeighborIds(activeEdgeDirection).collectAsMap().map(x =>(x._1->x._2.toList))    
  
  var allVertices= nodes.map(x =>x._1).collect()
  graph.cache()
  
graph.vertices.collect().foreach{
      t=> {
       
       rootVertex = t._1
    
      
       val initialGraph = graph.partitionBy(partitionStrategy, partitionCount).mapVertices((id, attr) => if (id == rootVertex) 0.0 else Double.PositiveInfinity)   
  
    	graph.unpersist(blocking = false)		
		initialGraph.cache()	
		

		var bfs = initialGraph.pregel(initialMessage, maxIterations, activeEdgeDirection)(vprog, sendMessage, reduceMessage)
		//println("bfs edges")
		//bfs.edges.collect().foreach{
      //   ee=> println(ee)
     //  }
		/*println("triplets")
		bfs.triplets.collect().foreach{
         tt => println(tt)
       }*/
		var labelNode : scala.collection.mutable.HashMap[VertexId, (Int)] =    scala.collection.mutable.HashMap.empty[VertexId, (Int)] //store label and credit of nodes		
		var creditNode : scala.collection.mutable.HashMap[VertexId, (Double)] = scala.collection.mutable.HashMap.empty[VertexId, (Double)]
		var locCreditEdge :  scala.collection.mutable.HashMap[(VertexId,VertexId), (Double)]= scala.collection.mutable.HashMap.empty[(VertexId,VertexId), (Double)]
		//val emptyMap: HashMap[String,String] = HashMap.empty[String,String]
		
		//var bfsTree =scala.collection.mutable.HashMap[Int,  scala.collection.mutable.Set[VertexId]]() //level and vertexId at level
		 var bfsTree = bfs.vertices.map(t=>(t._2.toInt -> t._1)).aggregateByKey(ListBuffer.empty[VertexId])((numList, num) => {numList += num; numList},
         (numList1, numList2) => {numList1.appendAll(numList2); numList1}).mapValues(_.toList).collect.toMap
         var parentInfo = scala.collection.mutable.HashMap[VertexId, List[VertexId]]()
    var allVerticesLevel = bfs.vertices.map(t=>(t._1 -> t._2.toInt )).collect.toMap
    
    //allVerticesLevel.take(10).foreach(println)
   
     allVertices.foreach{
      t=>{
       
        //t refers to vertex
        var tParentLevel =  allVerticesLevel(t)-1  // t parents in current bfs 
        if(tParentLevel == -1) 
         parentInfo.put(t, List(-1)) //making parent of root as -1 
        else 
        {
         var NodesAboveMe = bfsTree(tParentLevel)
     
         var Nodeneighbours = adjNeighbours(t)// t neighbours  
   
         parentInfo.put(t, NodesAboveMe.intersect(Nodeneighbours))
         //tempset = insetection of NodesAboveMe and Nodeneighbours
        }
             
        
      }
    }
    
   
		
		initialGraph.unpersist(blocking=false) 
		
		//labeleling nodes 
		
		
		labelNode.put(rootVertex,1) //add  1 to root
		val maxLevel  = bfsTree.size -1
		 for (i <- 1 to maxLevel )
		 {
		    var levelNodes = bfsTree(i)
		   // println(levelNodes)
		    levelNodes.foreach{
		      l => { 
		        //get parents of l (sum the labels of its parents)
		         var tempPInfo = parentInfo(l)
		        // println(tempPInfo)
		         	//	println(labelNode)
		         var sum = 0
		        tempPInfo.foreach{
		           tempP => { if(labelNode.keySet.contains(tempP)) sum = sum+ labelNode(tempP)}
		        }
		        labelNode.put(l,sum)}
		    }
		 }
		
		//println("printing label node")
		//println(labelNode)
		
		//get leaf nodes 
 var ching = graph.vertices.collect().map(t=>t._1).toList
		//println("ching "+ ching)
 
	var leaf =  ching.filterNot(parentInfo.values.flatten.toSet)
		
	
		
		
		
		
for (i <- maxLevel to 1 by -1 )
		 {
		    var levelNodes = bfsTree(i)
		   
		     levelNodes.foreach{
		      l => { 
		     
		        if(leaf.contains(l))
		          creditNode.put(l,1) // adding credit of 1 to leaf nodes 
		        else  //non leaf nodes
		        {
		            var sum = 1.0
		     var dagEdges = creditEdge.keys.filter(x => {(x._1 == l & allVerticesLevel(l) < allVerticesLevel(x._2) )  | (x._2 == l & allVerticesLevel(l) < allVerticesLevel(x._1))}) 
		       
		            dagEdges.foreach{
		              de=> {if(locCreditEdge.keySet.contains(de))sum = sum+ locCreditEdge(de)}
		            }	
		          // var dagEdges = graph.edges.filter(e=> {(e.srcId == l | e.dstId ==1)}).collect()
		                    
		           
		           
		         
		        creditNode.put (l, sum)
		          
		        }
		        
		        //add credits to the dag edges for these nodes at level i 
		        //check if l has one parent  or more
		        //get parents of l 
		         var tempPInfo = parentInfo(l)
		         if(tempPInfo.length == 1) 
		         {
		           var b = tempPInfo(0)	        
		           
		           
		var dagEdges = creditEdge.keys.filter(x => {(x._1 == l &  x._2 == b ) | (x._1 == b & x._2 == l) })
		
		            dagEdges.foreach{
		             
		                
		              de=>{
		                var current = creditNode(l)
		                locCreditEdge(de) = current		                
		                creditEdge(de) = creditEdge(de) +  current }
		            }	
		        
		           
		           
		          
		          // println("inside loop")
		          //println(creditEdge)
		           
		         }
		         else  //credit division
		         {
		           var totParents  = tempPInfo.length
		           var den = 0.0 
		            tempPInfo.foreach{
		             p =>{den = den + labelNode(p) 	               
		             }
		             }
		          
		           tempPInfo.foreach{
		             b =>{
		               var edgeCredit = ( labelNode(b) * creditNode(l))/ den
		               var dagEdges = creditEdge.keys.filter(x => {(x._1 == l &  x._2 == b ) | (x._1 == b & x._2 == l) })
		             
		            dagEdges.foreach{
		              de=>{
		                
		                locCreditEdge(de) = edgeCredit		                
		                creditEdge(de) = creditEdge(de) +  edgeCredit
		               }
		            }	
		               
		             }
		           }
		           
		         }
		        
		      }
		     
		    }
		 } 
		
 
 
	//println("printing credit node")
		//println(creditNodeList)
		
		//	println(labelNodeList)
				  
//	println(creditEdge)
    
    //println(bfsTree)
		
		//label using bfs 
		
		//println("1st tuple "+ bfs.vertices.first()._1)
			//println("2nd tuple "+bfs.vertices.first()._2)


      }
   }  //end of each vertex 
	
    
 
var result =  sparkContext.parallelize(creditEdge.toSeq.sortBy(_._1)).map(x =>(x._1._1,x._1._2,BigDecimal(x._2/2).setScale(1, BigDecimal.RoundingMode.FLOOR))).collect()	
		val outputFile = new PrintWriter(new File(args(1)+"Lakshmi_Chirumamilla_betweenness.txt"))      
    result.foreach{       x=> outputFile.println(x)    }
 outputFile.close()	
    
   } // end of main
   
   
  
}
