#time
#r "nuget: Akka.FSharp"

open System
open System.Diagnostics
open Akka.FSharp
open Akka.Actor

let ConvergenceRatio = 0.0000000001
let ListenLimit = 10
let squares = List.init 1000 (fun x -> (x+1)*(x+1))

type NodeMessages = 
    | SetNeighbours of List<IActorRef>
    | Gossip
    | ComputePathSum of float*float
type MasterMessages = 
    | Request of int*string*string
    | GossipComplete
    | ConvergenceReached of float
type TimerMessages = 
    | Start
    | Stop
    | PrintTimeTaken

type PathSumParams = {
    S: float
    W: float
    Neighbours: List<IActorRef>
    ConvergenceCount: int
}

let gossipNodeActor name (mailbox: Actor<_>) = 
    let randomSelector = System.Random()
    let rec loop isActive neighbours gossipCount = actor {
        let! message = mailbox.Receive()
        match message with
            | SetNeighbours(nodeList) -> 
                return! loop isActive nodeList gossipCount
            | Gossip -> 
                if not isActive then
                    return! loop isActive neighbours gossipCount
                if gossipCount > ListenLimit then
                    // printfn "I've heard enough! : %s" name
                    mailbox.Context.Parent <! GossipComplete
                    return! loop false neighbours gossipCount
                else 
                    (neighbours.Item (randomSelector.Next(0, neighbours.Length))) <! Gossip
                    mailbox.Self <! Gossip
                    if mailbox.Sender()=mailbox.Self then
                        return! loop isActive neighbours gossipCount
                    else 
                        return! loop isActive neighbours (gossipCount+1)
            | _ -> printfn "That message isn't for me."
    }
    loop true [] 0

let pathSumNodeActor name (mailbox: Actor<_>) = 
    let rand = System.Random()
    let rec loop {S=s; W=w; Neighbours=neighbours; ConvergenceCount=count} = actor {
        let! msg = mailbox.Receive()
        match msg with
            | SetNeighbours(nodeList) -> 
                return! loop {S=s; W=w; Neighbours=nodeList;ConvergenceCount=count}
            | ComputePathSum(sVal, wVal) ->
                let newS = (sVal+s)
                let newW = (wVal+w)
                let ratio = newS/newW
                let oldRatio = s/w
                // printfn "%s ||| %f/%f -> %f : %f -> %f" name s w newS newW ratio
                if count>=3 then
                    mailbox.Context.Parent <! ConvergenceReached(ratio)
                    return! loop {S=s;W=w;Neighbours=neighbours; ConvergenceCount=count}
                neighbours.Item(rand.Next(0, neighbours.Length)) <! ComputePathSum(newS/2.0, newW/2.0)
                if (abs (oldRatio - ratio) < ConvergenceRatio) then
                    return! loop {S=newS/2.0;W=newW/2.0;Neighbours=neighbours; ConvergenceCount=count+1}
                return! loop {S=newS/2.0; W=newW/2.0; Neighbours=neighbours; ConvergenceCount=0}
            | _ -> printfn "Guess it's not for me..."
    } 
    // For sum initiate with W=0 for all actors.
    loop {S=(float name); W=0.0; Neighbours=[];ConvergenceCount=0}
    // For average initiate with W=1 for all actors.
    // loop {S=(float name); W=1.0; Neighbours=[];ConvergenceCount=0}

let createFullTopology (nodeList:List<_>) = 
    printfn "Creating Full topology"
    for node in nodeList do
        let neighbours = List.filter (fun x -> x<>node) nodeList
        node <! SetNeighbours(neighbours)

let create2DTopology (nodeList:List<_>) =
    printfn "Creating 2D topology"
    let rowSize = int <| sqrt (float nodeList.Length)
    for i in [0..nodeList.Length-1] do
        let indices = (List.filter (fun x -> (x>=0 && x<nodeList.Length && not ((i%rowSize=0 && x=i-1) || ((i+1)%rowSize=0 && x=i+1)))) [i-1; i+1; i-rowSize; i+rowSize])
        let neighbours = [for i in indices do yield nodeList.Item(i)]
        nodeList.Item(i) <! SetNeighbours(neighbours)
        
let createLineTopology (nodeList:List<_>) =
    printfn "Creating Line topology"
    // if nodeList.Length>1 then
    nodeList.Item(0) <! SetNeighbours([nodeList.Item(1)])
    nodeList.Item(nodeList.Length-1) <! SetNeighbours([nodeList.Item(nodeList.Length-2)])
    for i in [1..nodeList.Length-2] do
        let neighbours = [nodeList.Item(i-1); nodeList.Item(i+1)]
        nodeList.Item(i) <! SetNeighbours(neighbours)

let createImp2DTopology (nodeList:List<_>) =
    printfn "Creating Imperfect 2D topology"
    let rowSize = int <| sqrt (float nodeList.Length)
    let random = System.Random()
    for i in [0..nodeList.Length-1] do
        let indices = (List.filter (fun x -> (x>=0 && x<nodeList.Length && not ((i%rowSize=0 && x=i-1) || ((i+1)%rowSize=0 && x=i+1)))) [i-1; i+1; i-rowSize; i+rowSize])
        let neighbours = [for i in indices do yield nodeList.Item(i)]
        let mutable randomIndex = random.Next(0, nodeList.Length)
        while(List.contains randomIndex indices) do
            randomIndex <- random.Next(0, nodeList.Length)
        nodeList.Item(i) <! SetNeighbours(List.append neighbours [nodeList.Item(randomIndex)])

let getNumberOfNodes numberOfNodes topology = 
    if topology = "2D" || topology = "imp2D" then
        let nodes = List.find (fun x -> x>=numberOfNodes) squares
        nodes
    else
        numberOfNodes

let getActorForAlogrithm algorithm = 
    if algorithm = "gossip" then
        gossipNodeActor
    else
        pathSumNodeActor

let handleNewRequest (numberOfNodes: int, topology:String, algorithm:string, mb:Actor<_>) = 
    printfn "Handling request"
    let nodeActor = getActorForAlogrithm(algorithm)        
    let nodeList = List.init (getNumberOfNodes numberOfNodes topology) (fun x -> spawn mb.Context (string x) <| nodeActor (string x))
    match topology with
        | "full" -> createFullTopology(nodeList)
        | "2D" -> create2DTopology(nodeList)
        | "line" -> createLineTopology(nodeList)
        | "imp2D" -> createImp2DTopology(nodeList)
        | _  -> printfn "Invalid topology"
    nodeList

let supervisorActor timerActor (mailbox: Actor<_>) =
    let randomNodeSelector = System.Random()
    let mutable convergenceCount = 0
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        
        match box msg with
        | :? MasterMessages as message ->
            // printfn "Message: %A" message
            match message with
                | Request(numberOfNodes, topology, algo) -> 
                    printfn "Toplogy : %s" topology
                    let nodeList = handleNewRequest(numberOfNodes, topology, algo, mailbox)
                    convergenceCount <- 0
                    timerActor <! Start
                    let index = randomNodeSelector.Next(0, nodeList.Length)
                    let startNode = nodeList.Item (index)
                    if algo = "gossip" then
                        startNode <! Gossip
                    else
                        // For sum calculation start with W=1.0
                        startNode <! ComputePathSum(0.0,1.0)
                        // For average calculation start with W=0.0
                        // startNode <! ComputePathSum(0.0,0.0)
                | GossipComplete ->
                    timerActor <! Stop
                    convergenceCount <- convergenceCount+1
                    if not(mailbox.Context.ReceiveTimeout.HasValue) then
                        // printfn "Setting timeout"
                        mailbox.Context.SetReceiveTimeout(TimeSpan.FromSeconds 5.0)
                | ConvergenceReached(sum) -> 
                    printfn "Convergence Reached! with sum: %.10f" sum
                    timerActor <! Stop
                    timerActor <! PrintTimeTaken
        | :? Akka.Actor.ReceiveTimeout -> 
            printfn "Received Timeout: with count %i" convergenceCount
            timerActor <! PrintTimeTaken
            mailbox.Context.SetReceiveTimeout(Nullable())
        | _ -> printfn "Got unknown message"
        
                
        return! loop()
    }
    loop()


let timer (mailbox: Actor<_>) = 
    let clock = Stopwatch()
    let rec loop latestStop = actor {
        let! msg = mailbox.Receive()
        match msg with 
            | Start -> clock.Start()
            | Stop ->  return! loop clock.ElapsedMilliseconds 
            | PrintTimeTaken -> printfn "Time taken: %dms" latestStop
        return! loop latestStop
    }
    loop <|int64 0

let system = ActorSystem.Create("ActorFactory")

let timeKeeper = spawn system "timer" timer

let master = spawn system "supervisor" <| supervisorActor timeKeeper

let args = fsi.CommandLineArgs
if args.Length < 4 then
    printfn "Usage: dotnet fsi /langversion:preview  .\\TestGossip.fsx {number of nodes} {topology} {algorithm}"
else
    let numberOfNodes = int args.[1] 
    let topology = args.[2]
    let algorithm = args.[3]

    master <! Request(numberOfNodes, topology,algorithm)
    Console.ReadKey() |> ignore

system.Terminate()