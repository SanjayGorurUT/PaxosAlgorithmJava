package paxos;

import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is the main class you need to implement paxos instances.
 * It corresponds to a single Paxos peer.
 */

public class Paxos implements PaxosRMI, Runnable {
    ReentrantLock mutex;
    String[] peers; // hostnames of all peers
    int[] ports; // ports of all peers
    int me; // this peer's index into peers[] and ports[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead; // for testing
    AtomicBoolean unreliable; // for testing

    // class vars
    static int numProposalsMade = 0;

    // per instance 
    HashMap<Integer, State> seqToState = new HashMap<>();
    // key: seq, value: state
    HashMap<Integer, Object> seqToVal = new HashMap<>();
    // key: seq, value: value
    HashMap<Long, Integer> threadSeq = new HashMap<>();
    // key: thread id, value: seq
    HashMap<Integer, ArrayList<Integer>> seqProposerInts = new HashMap<>();
    // key: seq, value: {proposalNumber, highestAcceptedProposal}
    HashMap<Integer, ArrayList<Object>> seqProposerObjs = new HashMap<>();
    // key: seq, value: {proposalValue, highestAcceptedProposalValue}
    HashMap<Integer, ArrayList<Integer>> seqAcceptInts = new HashMap<>();
    // key: seq, value: {highestProposalSeen, highestAcceptSeen}
    HashMap<Integer, Object> seqAcceptObjs = new HashMap<>();
    // key: seq, value: {highestAcceptValueSeen}

    int calledDone = -1;
    int[] seqDoneVals;

    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports) {

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);
        this.seqDoneVals = new int[peers.length];
        for(int i = 0; i < this.seqDoneVals.length; i++) {
            this.seqDoneVals[i] = -1;
        }

        // Your initialization code here

        // register peers, do not modify this part
        try {
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id) {
        Response callReply = null;

        PaxosRMI stub;
        try {
            Registry registry = LocateRegistry.getRegistry(this.ports[id]);
            stub = (PaxosRMI) registry.lookup("Paxos");
            if (rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if (rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if (rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch (Exception e) {
            return null;
        }
        return callReply;
    }

    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value) {
        // Your code here
        if(seq < this.Min()) {return;}
        Thread t1 = new Thread(this);
        this.seqToState.put(seq, State.Pending);
        this.seqToVal.put(seq, null);
        this.threadSeq.put(t1.getId(), seq);
        ArrayList<Integer> proposalInts = new ArrayList<>();
        proposalInts.add(numProposalsMade++); // proposal number
        proposalInts.add(-1); // highest accepeted proposal
        this.seqProposerInts.put(seq, proposalInts);
        ArrayList<Object> proposalObjs = new ArrayList<>();
        proposalObjs.add(value); // proposal value
        proposalObjs.add(-1); // highestAcceptedProposalValue;
        this.seqProposerObjs.put(seq, proposalObjs);
        ArrayList<Integer> acceptInts = new ArrayList<>();
        acceptInts.add(-1); // highest proposal seen
        acceptInts.add(-1); // highest accept seen
        this.seqAcceptInts.put(seq, acceptInts);
        this.seqAcceptObjs.put(seq, null); // highestAcceptValueSeen

        t1.start();
    }

    @Override
    public void run() {
        // Your code here
        int prepareResponses = 0;
        int acceptResponses = 0;
        int seq = this.threadSeq.get(Thread.currentThread().getId());
        int proposalNumber = this.seqProposerInts.get(seq).get(0);
        int highestAcceptedProposal = this.seqProposerInts.get(seq).get(1);
        Object proposalValue = this.seqProposerObjs.get(seq).get(0);
        while (this.seqToState.get(seq) != State.Decided) {
            // System.out.println(this.peers.length);
            for (int i = 0; i < this.peers.length; i++) {
                Response response = this.Call("Prepare", new Request(proposalNumber, proposalValue, seq, calledDone, this.me), i);
                if (response != null) {    
                    prepareResponses = (response.isOk) ? (prepareResponses + 1) : prepareResponses;
                    this.seqDoneVals[i] = response.done;
                    if(response.isOk && response.highestAcceptedProposalNumber > highestAcceptedProposal 
                        && (response.highestAcceptedProposalValue) != null) {
                        proposalValue = response.highestAcceptedProposalValue;
                    }
                }
            }
            if (prepareResponses > (peers.length/2)) {
                for(int i = 0; i < this.peers.length; i++) {

                    Response response = this.Call("Accept", new Request(proposalNumber, proposalValue, seq, calledDone, this.me), i);
                    if (response != null) {
                        this.seqDoneVals[i] = response.done;
                        acceptResponses = (response.isOk) ? (acceptResponses + 1) : acceptResponses;
                    }
                }
                if(acceptResponses > (peers.length/2)) {
                    for(int i = 0; i < this.peers.length; i++) {
                        Response response = this.Call("Decide", new Request(proposalNumber, proposalValue, seq, calledDone, this.me), i);
                        if (response != null) {
                            this.seqDoneVals[i] = response.done;
                        }
                    } 
                    this.seqToState.put(seq, State.Decided);
                    // System.out.println("Here");
                    this.seqToVal.put(seq, proposalValue);
                }
            }
        }
    }

    // RMI Handler for prepare requests
    public Response Prepare(Request req) {
        // your code here

        int seq = req.seq;
        if (!seqToState.containsKey(seq)) {
            this.seqToState.put(seq, State.Pending);
            this.seqToVal.put(seq, null);
            this.threadSeq.put(Thread.currentThread().getId(), seq);
            ArrayList<Integer> acceptInts = new ArrayList<>();
            acceptInts.add(-1); // highest proposal seen
            acceptInts.add(-1); // highest accept seen
            this.seqAcceptInts.put(seq, acceptInts);
            this.seqAcceptObjs.put(seq, null); // highestAcceptValueSeen
        }

        int highestProposalSeen = this.seqAcceptInts.get(seq).get(0);
        int highestAcceptSeen = this.seqAcceptInts.get(seq).get(1);
        Object highestAcceptValueSeen = this.seqAcceptObjs.get(seq);
        this.seqDoneVals[req.me] = req.done;


        if(req.proposalNumber > highestProposalSeen) {
            this.seqAcceptInts.get(seq).set(0, req.proposalNumber);
            return new Response(true, req.proposalNumber, highestAcceptSeen, highestAcceptValueSeen, calledDone);
        }
        else {
            return new Response(false, req.proposalNumber, highestAcceptSeen, highestAcceptValueSeen, calledDone);
        }
    }

    // RMI Handler for accept requests
    public Response Accept(Request req) {
        // your code here
        int seq = req.seq;
        int highestProposalSeen = this.seqAcceptInts.get(seq).get(0);
        this.seqDoneVals[req.me] = req.done;

        if(req.proposalNumber >= highestProposalSeen) {
            this.seqAcceptInts.get(seq).set(0, req.proposalNumber);
            this.seqAcceptInts.get(seq).set(1,  req.proposalNumber);
            this.seqAcceptObjs.put(seq, req.proposalValue);
            return new Response(true, req.proposalNumber,  this.seqAcceptInts.get(seq).get(1), this.seqAcceptObjs.get(seq), calledDone);
        }
        else {
            return new Response(false, -1, -1, null, calledDone);
        }
    }

    // RMI Handler for decide requests
    public Response Decide(Request req) {
        // your code 
        this.seqToState.put(req.seq, State.Decided);
        this.seqToVal.put(req.seq, req.proposalValue);
        this.seqDoneVals[req.me] = req.done;
        return new Response(calledDone);
    }

    public void updateHighestDone() {
        int seq = this.Min();
        Set<Integer> findLess = new HashSet<>();

        for(HashMap.Entry<Integer, State> entry: this.seqToState.entrySet()) {
            if(entry.getKey() < seq) {
                findLess.add(entry.getKey());
            }
        }
        this.seqToState.keySet().removeAll(findLess);
        findLess.clear();

        for(HashMap.Entry<Integer, Object> entry: this.seqToVal.entrySet()) {
            if(entry.getKey() < seq) {
                findLess.add(entry.getKey());
            }
        }
        this.seqToVal.keySet().removeAll(findLess);
        findLess.clear();

        // for(HashMap.Entry<Long, Integer> entry: this.threadSeq.entrySet()) {
        //     if(entry.getValue() < seq) {
        //         findLess.add(entry.getValue());
        //     }
        // }
        // this.threadSeq.values().removeAll(findLess);
        // findLess.clear();

        for (int each: this.seqProposerInts.keySet()) {
            if (each < seq) {
                findLess.add(each);
            }
        }
        this.seqProposerInts.keySet().removeAll(findLess);
        findLess.clear();

        for (int each: this.seqAcceptInts.keySet()) {
            if (each < seq) {
                findLess.add(each);
            }
        }
        this.seqAcceptInts.keySet().removeAll(findLess);
        findLess.clear();

        for (int each: this.seqProposerObjs.keySet()) {
            if (each < seq) {
                findLess.add(each);
            }
        }
        this.seqProposerObjs.keySet().removeAll(findLess);
        findLess.clear();

        for (int each: this.seqAcceptObjs.keySet()) {
            if (each < seq) {
                findLess.add(each);
            }
        }
        this.seqAcceptObjs.keySet().removeAll(findLess);
        findLess.clear();
    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        if(calledDone > seq) return;
        calledDone = seq;
        this.seqDoneVals[this.me] = calledDone;
    }

    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max() {
        // Your code here
        int largestSeq = -1;
        for (int each: this.seqToState.keySet()) {
            largestSeq = Math.max(largestSeq, each);
        }
        return largestSeq;
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().

     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.

     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.

     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min() {
        // Your code here
        int min = Integer.MAX_VALUE;
        for(int i = 0; i < this.seqDoneVals.length; i++) {
            if(min > this.seqDoneVals[i]) {
                min = this.seqDoneVals[i];
            }
        }
        return min + 1;
    }

    /**
     * The application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq) {
        // Your code here
        if(seq < this.Min()) return new retStatus(State.Forgotten, null);
        if(this.seqToState.containsKey(seq) && this.seqToVal.containsKey(seq)) {
            // System.out.println(seq + " " + this.seqToVal.get(seq));
            return new retStatus(this.seqToState.get(seq), this.seqToVal.get(seq));
        }
        else if(this.seqToState.containsKey(seq)) {
            return new retStatus(State.Pending, null);
        }
        return new retStatus(null, null);
    }

    /**
     * helper class for Status() return
     */
    public class retStatus {
        public State state;
        public Object v;

        public retStatus(State state, Object v) {
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill() {
        this.dead.getAndSet(true);
        if (this.registry != null) {
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch (Exception e) {
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead() {
        return this.dead.get();
    }

    public void setUnreliable() {
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable() {
        return this.unreliable.get();
    }
}
