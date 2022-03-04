import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    private ZooKeeper zooKeeper;
    private String currentZnodeName;


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();

        //volunteer For Leadership
        leaderElection.volunteerForLeadership();

        // elect the leader
        leaderElection.electLeader();

        // make main thread waits
        leaderElection.run();

        leaderElection.close();
        System.out.println("Disconnected from Zookeeper");
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_"; // c stands for candidate
        // create a new znode
        String znodeFullPath = zooKeeper.create(znodePrefix,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Created znode : "+znodeFullPath);
        // extract the znode name from the path
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE+"/","");
    }

    public void electLeader() throws InterruptedException, KeeperException {
        /*
        * If the parent node does not exist in the ZooKeeper, a KeeperException with error code KeeperException.NoNode will be thrown.

        * An ephemeral node cannot have children. If the parent node of the given path is ephemeral, a KeeperException with error code KeeperException.NoChildrenForEphemerals will be thrown.

        * This operation, if successful, will trigger all the watches left on the node of the given path by exists and getData API calls, and the watches left on the parent node by getChildren API calls.
        * */
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE,false); // without watching 'em

        // sort the children
        Collections.sort(children);

        // the smallest child will be the first on the list
        String smallestChild = children.get(0);

        // figure out if I am the leader on not
        if (smallestChild.equals(currentZnodeName)){
            System.out.println("I am the leader");
            return;
        }else{
            // I am not the leader
            System.out.println("I am not the leader, " +smallestChild+ " is the leader");

        }

    }

    public void connectToZookeeper() throws IOException {
        // create zookeeper object
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS,SESSION_TIMEOUT,this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }


    @Override
    // handling events in the event handling thread
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()){

            // handle connections and disconnections events
            case None:
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    // if we get connected to the Zookeeper server
                    System.out.println("Successfully connected to Zookeeper server");
                }else{
                    // when we lose connection to zookeeper
                    synchronized (zooKeeper){
                        System.out.println("Disconnected from zookeeper event");
                        // wake up the main thread
                        zooKeeper.notifyAll();
                    }
                }
                break;
        }
    }
}
