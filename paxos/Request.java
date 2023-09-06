package paxos;

import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the request message for each RMI call.
 * Hint: You may need the sequence number for each paxos instance and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 * Hint: It is easier to make each variable public
 */
// Prepare
// Accept
// Propose

public class Request implements Serializable {
    static final long serialVersionUID = 1L;

    // Your data here
    int proposalNumber;
    Object proposalValue;
    int seq;
    int done;
    String info = null;
    int me;

    // Your constructor and methods here
    public Request(int proposalNumber, Object proposalValue) {
        this.proposalNumber = proposalNumber;
        this.proposalValue = proposalValue;
    }

    public Request(int proposalNumber, Object proposalValue, int seq, int done, int me) {
        this.proposalNumber = proposalNumber;
        this.proposalValue = proposalValue;
        this.seq = seq;
        this.done = done;
        this.me = me;
    }

    public Request(String info) {
        this.info = info;
    }
}
