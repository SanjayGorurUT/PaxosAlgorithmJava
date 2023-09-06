package paxos;

import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the response message for each RMI call.
 * Hint: You may need a boolean variable to indicate ack of acceptors and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 */
public class Response implements Serializable {
    static final long serialVersionUID = 2L;

    boolean isOk;
    int proposalNumber;
    int highestAcceptedProposalNumber;
    Object highestAcceptedProposalValue;
    int done;

    public Response(boolean isOk, int proposalNumber, int highestAcceptedProposalNumber, Object highestAcceptedProposalValue, int done) {
        this.isOk = isOk;
        this.proposalNumber = proposalNumber;
        this.highestAcceptedProposalNumber = highestAcceptedProposalNumber;
        this.highestAcceptedProposalValue = highestAcceptedProposalValue;
        this.done = done;
    }

    public Response(int done) {
        this.done = done;
    }
}
