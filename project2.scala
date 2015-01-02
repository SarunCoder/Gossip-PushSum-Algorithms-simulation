import akka.actor._
import scala.util.Random
import java.util.List
import java.util.ArrayList
import scala.collection.mutable.ListBuffer
import java.lang.ArrayIndexOutOfBoundsException
import java.lang.IndexOutOfBoundsException


/**
  * Main singleton class which simulates the project.
  */

object project2 {
  sealed trait bMessage
  
  /**
   * Node messages between node actors.
   */
  case class StartGossip(rumour : String) extends bMessage
  case class PropagateRumour(rumour : String) extends bMessage
  case class StartPushSum(si:Double,wi:Double) extends bMessage
  case class WakeMeUp() extends bMessage
  /**
   * Master messages between the master controller actor and nodes in the network.
   */
  case class StartSimulation(rumour : String) extends bMessage
  case class RumourReceived(rumour : String, actorId : Int) extends bMessage
  case class RumourLimitReached(actorId : Int) extends bMessage
  case class PushSum(si:Double,wi:Double) extends bMessage
  case class PushSumConverged(actorId : Int,lastRatio:Double) extends bMessage

  var masterActor:ActorRef = null
  val system = ActorSystem("MasterSystem")
  val RUMOURMESSAGE:String = "This is a rumour"
  val RUMOURECEIVELIMIT:Int = 20
  
  def main(args : Array[String]){
    
    if(args.length != 3){
      System.out.println("Incorrect number of arguments")
      sys.exit()
    }
    else if(args(0).toInt < 1){
      System.out.println("Illegal aurgument : Incorrect number of nodes specified")
      sys.exit()
    }
    else if(args(1).toLowerCase() != "2d" && args(1).toLowerCase() != "line" && args(1).toLowerCase() != "full" && args(1).toLowerCase() != "imp2d"){
      System.out.println("Illegal aurgument : Incorrect topology specified")
      sys.exit()
    }
    else if(args(2).toLowerCase() != "gossip" && args(2).toLowerCase() != "push-sum"){
      System.out.println("Illegal aurgument : Incorrect Algorithm specified")
      sys.exit()
    }
    else{
      var numNodes = args(0).toInt
      if(args(1).toLowerCase() == "2d" || args(1).toLowerCase() =="imp2d"){
        val sqrtNum = math.ceil(math.sqrt(args(0).toDouble)).toInt
        numNodes = sqrtNum * sqrtNum
      }
      if(args(2).toLowerCase()=="push-sum"){
        masterActor = system.actorOf(Props(new Master(numNodes,args(1),args(2))), name = "Master")
        masterActor ! StartSimulation("")
        }
      else if(args(2).toLowerCase() =="gossip"){
        masterActor = system.actorOf(Props(new Master(numNodes,args(1),args(2))), name = "Master")
	    masterActor ! StartSimulation(RUMOURMESSAGE)
	    }
      }
  }
  
  /**
   * The master actor class.
   * Responsible for building the network and starting the simulation.
   * This might act as intermediate controller for control messages during network topology changes.
   */
  class Master(numOfActors:Int,topology:String,algorithm:String) extends Actor{
    
    var numberOfActorsCompleted:Int = 0
    var numberOfActorsReceived:Int = 0
    var numberOfActorsAlive:Array[Int] = new Array[Int](1)
    numberOfActorsAlive(0) = numOfActors
    var startTime:Long = _
    var endTime:Long = _
    var finishTime:Long = _
    val nodeSystem = ActorSystem("NodeSystem")
    val NodeCollection:Array[ActorRef]= new Array[ActorRef](numOfActors)
    var currentParticipants = new Array[Boolean](numOfActors)
    
    for(n <-0 to numOfActors-1)
    currentParticipants(n) = true
    
    for(n <- 0 to numOfActors-1){
      var propNode= Props(new Node(topology,algorithm,numOfActors,n,NodeCollection,currentParticipants,numberOfActorsAlive))
      NodeCollection(n) = nodeSystem.actorOf(propNode)
     }
    

    def receive ={
    
     case StartSimulation(rumourMessage) =>
       val randomStartNode:Int = Random.nextInt(numOfActors)
       if(algorithm.toLowerCase() == "gossip"){
         startTime = System.currentTimeMillis()
         System.out.println("*****Starting Gossip*****")
         NodeCollection(randomStartNode) ! StartGossip(rumourMessage)
         }
       else{
         startTime = System.currentTimeMillis()
         System.out.println("*****Starting PushSum*****")
         NodeCollection(randomStartNode) ! StartPushSum(0.0,0.0)
         //System.out.println("PushSum started at node: "+randomStartNode)
         }
 
     case PushSumConverged(actorId, ratio) =>
       numberOfActorsCompleted = numberOfActorsCompleted + 1
       if(numberOfActorsCompleted == 1){
         System.out.println("The Node "+actorId+" has reached limit : total actors:"+ numberOfActorsCompleted)
         System.out.println("The Ratio is : " + ratio)
         endTime = System.currentTimeMillis()
          System.out.println("The time taken for Pushsum CONVERGENCE with "+topology+ " topology: " +(endTime-startTime))
          shutDown()
          }
       
     case RumourReceived(rumourMessage,actorId) =>
       numberOfActorsReceived = numberOfActorsReceived + 1
       //System.out.println("The number of actors heard  : "+ numberOfActorsReceived)
       //System.out.println("The Node "+actorId+" has received the rumour: "+ rumourMessage)
       if(numberOfActorsReceived == numOfActors){
         endTime = System.currentTimeMillis()
         System.out.println("The time taken for Gossip CONVERGENCE for with "+topology+ " topology: "+ (endTime-startTime))
       }
       
     case RumourLimitReached(actorId) =>
       numberOfActorsCompleted = numberOfActorsCompleted+1
       //System.out.println("The Node "+actorId+" has reached limit : total actors:"+ numberOfActorsCompleted)
       currentParticipants(actorId) = false
       //System.out.println(currentParticipants.toString())
       //System.out.println("Continue program 1")
       if(numberOfActorsCompleted == (numOfActors-1)){
        // System.out.println("The Node "+actorId+" has reached limit || total actors reached limit: "+ numberOfActorsCompleted)
         finishTime = System.currentTimeMillis()
         System.out.println("The time taken for ALL NODES to COMPLETE with "+topology+ " topology: "+ (finishTime-startTime))
         shutDown()
       }
       
    }
    
    def shutDown() ={
      System.out.println("Stopping : Shutting down system")
      System.out.println("****************")
      System.out.println("***************")
      System.out.println("**************")
      System.out.println("*************")
      System.out.println("************")
      System.out.println("***********")
      System.out.println("**********")
      System.out.println("*********")
      System.out.println("********")
      System.out.println("*******")
      System.out.println("******")
      System.out.println("****")
      System.out.println("***")
      System.out.println("**")
      System.out.println("*")
      nodeSystem.shutdown()
      system.shutdown()
      sys.exit()
      System.out.println("All Over Done Finished")
    }
  }
  
  /**
   * The node actor class. Each node in the network is simulated as an actor.
   * Responsible for relaying the messages
   * In course of time, the nodes may go down temporarily or permanently and may come back to participate at a later time.
   * The network topology in which the nodes are connected plays a role on how the message route is determined.
   */
  class Node(networktopology:String,algorithm:String,numberOfActors:Int,actorId:Int, NodeCollection : Array[ActorRef], currentParticipants : Array[Boolean],numberOfActorsAlive:Array[Int]) extends Actor{

    var rumourReceivedCount:Int = 0
    var si = actorId.toDouble+1.0
    var wi = 1.0
    var prevRatio = 0.0
    var pushcount = 0


    def receive = {
      
      case WakeMeUp() =>
        var nextNeighbor = findNextNeigbor();
        if(currentParticipants(nextNeighbor)){
           
	           si = si/2.0
	           wi = wi/2.0
	           prevRatio = si/wi
	           NodeCollection(nextNeighbor) ! StartPushSum(si,wi)
	          
        }
        
      case StartPushSum(s,w) =>
        try{
          if(currentParticipants(actorId)){
	        var nextNeighbor:Int = 0 
	        val sinew:Double = si+s
	        val winew:Double = wi+w
	        if(math.abs((prevRatio) - (sinew/winew)) <= math.pow(10,-10)){
	          pushcount = pushcount+1
	          if(pushcount == 3){
	            masterActor ! PushSumConverged(actorId, sinew/winew)
	            currentParticipants(actorId) = false
	            context.stop(self)
	            }
	          }
	        else{
	          pushcount = 0
	          }
	        if(currentParticipants(actorId)){
	          
	          nextNeighbor = findNextNeigbor();
	          if(currentParticipants(nextNeighbor)){
	            si = sinew/2.0
	            wi = winew/2.0
	            prevRatio = si/wi
	            NodeCollection(nextNeighbor) ! StartPushSum(si,wi)
	             }
		         import system.dispatcher
		         val duration = scala.concurrent.duration.FiniteDuration(1,"milliseconds")
		         system.scheduler.scheduleOnce(duration,self,WakeMeUp())
	          }
	        }
        }
        catch{
          case e:Exception =>
           System.out.print("")
        }


      case StartGossip(rumourMessage) =>
        var nextNeighbor = 0
        rumourReceivedCount = rumourReceivedCount + 1
        masterActor ! RumourReceived(rumourMessage,actorId)
        nextNeighbor = findNextNeigbor();
        NodeCollection(nextNeighbor) ! PropagateRumour(rumourMessage)
        
        
      case PropagateRumour(rumourMessage) =>
        try{
           var nextNeighbor = 0 
          if(currentParticipants(actorId)){

              if(!sender.equals(self)){
                //System.out.println("Working")
                rumourReceivedCount = rumourReceivedCount + 1
              }
              
              
	          if(rumourReceivedCount == 1 && !sender.equals(self)){
	            masterActor ! RumourReceived(rumourMessage,actorId)
	          }
	          else if(rumourReceivedCount == RUMOURECEIVELIMIT && currentParticipants(actorId)){
	            masterActor ! RumourLimitReached(actorId)
	            //System.out.println("Continue program 1.1")
	            currentParticipants(actorId) = false
	            numberOfActorsAlive(0) = numberOfActorsAlive(0)-1
	            //System.out.println("Continue program 2")
	          }
	          

	          if(currentParticipants(actorId)){
	            nextNeighbor = findNextNeigbor();
	            if(currentParticipants(nextNeighbor)){
	              NodeCollection(nextNeighbor) ! PropagateRumour(rumourMessage)
	            }
	          }
	          else if(!currentParticipants(actorId) && (numberOfActorsAlive(0) != 1)){
	            //System.out.println("Continue program 4")
	            //System.out.print("Finding next neigbour for actor:"+actorId)
	            var nextNeighbor = findNextNeigbor();
	            //System.out.println("Found next neigbour for actor:"+actorId+" next neighbor is:"+nextNeighbor)
	            if(currentParticipants(nextNeighbor)){
	              NodeCollection(nextNeighbor) ! PropagateRumour(rumourMessage)
	              //context.stop(self)
	              }
	          }


	          if(currentParticipants(actorId)){
	            import system.dispatcher
	            val duration = scala.concurrent.duration.FiniteDuration(10,"milliseconds")
	            system.scheduler.scheduleOnce(duration,self,PropagateRumour(rumourMessage))
            }
          }
          
        }
        catch{
          case e:ArrayIndexOutOfBoundsException =>
            System.out.println("The actor id "+actorId+" tried to access neighbor when the system was shutting down....")
          case e:Exception =>
           // System.out.println ("The actor id "+actorId+" tried to access neighbor when the system was shutting down....")
            
        }
    }
    
    
    def findNextNeigbor():Int ={

      var randomNeighbor: Int = -1
      
      if(networktopology.toLowerCase() == "full" && (numberOfActorsAlive(0) != 1)){//Full topology
        var found = false
        //System.out.println("Finding random *****************")
        while(!found){
          randomNeighbor = Random.nextInt(numberOfActors)
          //System.out.println("Inside while *****************")
          if(randomNeighbor != actorId && currentParticipants(randomNeighbor)){
            found = true
            }
        }
      }

      else if(networktopology.toLowerCase() == "line"){//Line Topology
        if(actorId == 0){
          randomNeighbor = 1;
        }
        else if(actorId == (numberOfActors-1)){
          randomNeighbor = actorId-1
        }
        else{
          val rand = Random.nextInt(2);
          if(rand == 0){
            randomNeighbor = actorId-1
          }
          else{
            randomNeighbor = actorId+1
          }
        }
      }

      else if((networktopology.toLowerCase() == "2d")){//Perfect 2D Grid
        
        if(actorId == 0){//topleft
          //System.out.println("top left")
          val rand = Random.nextInt(2);
          if(rand == 0){
            randomNeighbor = actorId+1
          }
          else{
            randomNeighbor = Math.sqrt(numberOfActors).toInt
          }
        }
        else if(actorId == (numberOfActors-1)){//bottom right
          //System.out.println("Bot right")
          val rand = Random.nextInt(2);
          if(rand == 0){
            randomNeighbor = actorId-1
          }
          else{
            randomNeighbor = actorId - Math.sqrt(numberOfActors).toInt
          }
        }
        else if(actorId == (Math.sqrt(numberOfActors).toInt-1)){//top right
          //System.out.println("top right")
          val rand = Random.nextInt(2);
          if(rand == 0){
            randomNeighbor = actorId-1
          }
          else{
            randomNeighbor = actorId+Math.sqrt(numberOfActors).toInt
          }
        }
        else if(actorId == (numberOfActors - Math.sqrt(numberOfActors).toInt)){//bottom left
          //System.out.println("bot left")
          val rand = Random.nextInt(2);
          if(rand == 0){
            randomNeighbor = actorId+1
          }
          else{
            randomNeighbor = actorId - Math.sqrt(numberOfActors).toInt
          }
        }
        else if(actorId>0 && actorId<(Math.sqrt(numberOfActors)-1)){//top edge
          //System.out.println("top edge")
          val rand = Random.nextInt(3);
          if(rand == 0){
            randomNeighbor = actorId+1
          }
          else if(rand == 1){
            randomNeighbor = actorId-1
          }
          else{
            randomNeighbor = actorId + Math.sqrt(numberOfActors).toInt
          }
        }
        else if((actorId<numberOfActors-1) && actorId>(numberOfActors - Math.sqrt(numberOfActors).toInt)){//bottom edge
          //System.out.println("Bot edge")
          val rand = Random.nextInt(3);
          if(rand == 0){
            randomNeighbor = actorId+1
          }
          else if(rand == 1){
            randomNeighbor = actorId-1
          }
          else{
            randomNeighbor = actorId - Math.sqrt(numberOfActors).toInt
          }
        }
        else if(actorId%Math.sqrt(numberOfActors).toInt == 0 && actorId !=0 && actorId != (numberOfActors - Math.sqrt(numberOfActors).toInt)){//left edge
          //System.out.println("left edge")
          val rand = Random.nextInt(3);
          if(rand == 0){
            randomNeighbor = actorId+1
          }
          else if(rand == 1){
            randomNeighbor = actorId - Math.sqrt(numberOfActors).toInt
          }
          else{
            randomNeighbor = actorId + Math.sqrt(numberOfActors).toInt
          }
        }
        else if(actorId%Math.sqrt(numberOfActors).toInt == Math.sqrt(numberOfActors).toInt-1 && actorId !=Math.sqrt(numberOfActors).toInt-1 && actorId != (numberOfActors-1)){//right edge
          //System.out.println("right edge")
          val rand = Random.nextInt(3);
          if(rand == 0){
            randomNeighbor = actorId-1
          }
          else if(rand == 1){
            randomNeighbor = actorId - Math.sqrt(numberOfActors).toInt
          }
          else{
            randomNeighbor = actorId + Math.sqrt(numberOfActors).toInt
          }
        }
        else{
          //System.out.println("other")
          val rand = Random.nextInt(4);
          if(rand == 0){
            randomNeighbor = actorId-1
          }
          else if(rand == 1){
            randomNeighbor = actorId+1
          }
          else if(rand == 2){
            randomNeighbor = actorId - Math.sqrt(numberOfActors).toInt
          }
          else{
            randomNeighbor = actorId + Math.sqrt(numberOfActors).toInt
          }
          
        }
      }
     else if(networktopology.toLowerCase() == "imp2d"){//Imperfect 2D grid
        if(actorId == 0){//topleft
          //System.out.println("top left")
          val rand = Random.nextInt(3);
          if(rand == 0){
            randomNeighbor = actorId+1
          }
          else if(rand==1){
            var found = false
	        //System.out.println("Finding random *****************")
	        while(!found){
	          randomNeighbor = Random.nextInt(numberOfActors)
	          //System.out.println("Inside while *****************")
	          if(randomNeighbor != actorId && currentParticipants(randomNeighbor)){
	            found = true
	            }
	        }
          }
          else{
            randomNeighbor = Math.sqrt(numberOfActors).toInt
          }
        }
        else if(actorId == (numberOfActors-1)){//bottom right
          //System.out.println("Bot right")
          val rand = Random.nextInt(3);
          if(rand == 0){
            randomNeighbor = actorId-1
          }
          else if(rand==1){
            var found = false
	        //System.out.println("Finding random *****************")
	        while(!found){
	          randomNeighbor = Random.nextInt(numberOfActors)
	          //System.out.println("Inside while *****************")
	          if(randomNeighbor != actorId && currentParticipants(randomNeighbor)){
	            found = true
	            }
	        }
          }
          else{
            randomNeighbor = actorId - Math.sqrt(numberOfActors).toInt
          }
        }
        else if(actorId == (Math.sqrt(numberOfActors).toInt-1)){//top right
          //System.out.println("top right")
          val rand = Random.nextInt(3);
          if(rand == 0){
            randomNeighbor = actorId-1
          }
          else if(rand==1){
            var found = false
	        //System.out.println("Finding random *****************")
	        while(!found){
	          randomNeighbor = Random.nextInt(numberOfActors)
	          //System.out.println("Inside while *****************")
	          if(randomNeighbor != actorId && currentParticipants(randomNeighbor)){
	            found = true
	            }
	        }
          }
          else{
            randomNeighbor = actorId+Math.sqrt(numberOfActors).toInt
          }
        }
        else if(actorId == (numberOfActors - Math.sqrt(numberOfActors).toInt)){//bottom left
          //System.out.println("bot left")
          val rand = Random.nextInt(3);
          if(rand == 0){
            randomNeighbor = actorId+1
          }
          else if(rand==1){
            var found = false
	        //System.out.println("Finding random *****************")
	        while(!found){
	          randomNeighbor = Random.nextInt(numberOfActors)
	          //System.out.println("Inside while *****************")
	          if(randomNeighbor != actorId && currentParticipants(randomNeighbor)){
	            found = true
	            }
	        }
          }
          else{
            randomNeighbor = actorId - Math.sqrt(numberOfActors).toInt
          }
        }
        else if(actorId>0 && actorId<(Math.sqrt(numberOfActors)-1)){//top edge
          //System.out.println("top edge")
          val rand = Random.nextInt(4);
          if(rand == 0){
            randomNeighbor = actorId+1
          }
          else if(rand == 1){
            randomNeighbor = actorId-1
          }
          else if(rand==2){
            var found = false
	        //System.out.println("Finding random *****************")
	        while(!found){
	          randomNeighbor = Random.nextInt(numberOfActors)
	          //System.out.println("Inside while *****************")
	          if(randomNeighbor != actorId && currentParticipants(randomNeighbor)){
	            found = true
	            }
	        }
          }
          else{
            randomNeighbor = actorId + Math.sqrt(numberOfActors).toInt
          }
        }
        else if((actorId<numberOfActors-1) && actorId>(numberOfActors - Math.sqrt(numberOfActors).toInt)){//bottom edge
          //System.out.println("Bot edge")
          val rand = Random.nextInt(4);
          if(rand == 0){
            randomNeighbor = actorId+1
          }
          else if(rand == 1){
            randomNeighbor = actorId-1
          }
          else if(rand==2){
            var found = false
	        //System.out.println("Finding random *****************")
	        while(!found){
	          randomNeighbor = Random.nextInt(numberOfActors)
	          //System.out.println("Inside while *****************")
	          if(randomNeighbor != actorId && currentParticipants(randomNeighbor)){
	            found = true
	            }
	        }
          }
          else{
            randomNeighbor = actorId - Math.sqrt(numberOfActors).toInt
          }
        }
        else if(actorId%Math.sqrt(numberOfActors).toInt == 0 && actorId !=0 && actorId != (numberOfActors - Math.sqrt(numberOfActors).toInt)){//left edge
          //System.out.println("left edge")
          val rand = Random.nextInt(4);
          if(rand == 0){
            randomNeighbor = actorId+1
          }
          else if(rand == 1){
            randomNeighbor = actorId - Math.sqrt(numberOfActors).toInt
          }
          else if(rand==2){
            var found = false
	        //System.out.println("Finding random *****************")
	        while(!found){
	          randomNeighbor = Random.nextInt(numberOfActors)
	          //System.out.println("Inside while *****************")
	          if(randomNeighbor != actorId && currentParticipants(randomNeighbor)){
	            found = true
	            }
	        }
          }
          else{
            randomNeighbor = actorId + Math.sqrt(numberOfActors).toInt
          }
        }
        else if(actorId%Math.sqrt(numberOfActors).toInt == Math.sqrt(numberOfActors).toInt-1 && actorId !=Math.sqrt(numberOfActors).toInt-1 && actorId != (numberOfActors-1)){//right edge
          //System.out.println("right edge")
          val rand = Random.nextInt(4);
          if(rand == 0){
            randomNeighbor = actorId-1
          }
          else if(rand == 1){
            randomNeighbor = actorId - Math.sqrt(numberOfActors).toInt
          }
          else if(rand==2){
            var found = false
	        //System.out.println("Finding random *****************")
	        while(!found){
	          randomNeighbor = Random.nextInt(numberOfActors)
	          //System.out.println("Inside while *****************")
	          if(randomNeighbor != actorId && currentParticipants(randomNeighbor)){
	            found = true
	            }
	        }
          }
          else{
            randomNeighbor = actorId + Math.sqrt(numberOfActors).toInt
          }
        }
        else{
          //System.out.println("other")
          val rand = Random.nextInt(5);
          if(rand == 0){
            randomNeighbor = actorId-1
          }
          else if(rand == 1){
            randomNeighbor = actorId+1
          }
          else if(rand == 2){
            randomNeighbor = actorId - Math.sqrt(numberOfActors).toInt
          }
          else if(rand == 3){
            var found = false
	        //System.out.println("Finding random *****************")
	        while(!found){
	          randomNeighbor = Random.nextInt(numberOfActors)
	          //System.out.println("Inside while *****************")
	          if(randomNeighbor != actorId && currentParticipants(randomNeighbor)){
	            found = true
	            }
	        }
          }
          else{
            randomNeighbor = actorId + Math.sqrt(numberOfActors).toInt
          }
        }
      }
      return randomNeighbor
    }
  }

}