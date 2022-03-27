package cluster.management;

public interface OnElectionCAllback {
    void onElectedToBeLeader();
    void onWorker();
}
