import cluster.management.LeaderElectionIntegrated;
import cluster.management.ServiceRegistryIntegrated;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ApplicationIntegrated implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final int DEFAULT_PORT = 8080;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;

        ApplicationIntegrated application = new ApplicationIntegrated();
        ZooKeeper zooKeeper = application.connectToZookeeper();

        ServiceRegistryIntegrated serviceRegistry = new ServiceRegistryIntegrated(zooKeeper);

        OnElectionAction onElectionAction = new OnElectionAction(serviceRegistry,currentServerPort);

        // leader election
        LeaderElectionIntegrated leaderElection = new LeaderElectionIntegrated(zooKeeper,onElectionAction);
        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();

        // make main thread waits
        application.run();
        application.close();
        System.out.println("Disconnected from Zookeeper, exiting application");
    }


    public ZooKeeper connectToZookeeper() throws IOException {
        // create zookeeper object
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS,SESSION_TIMEOUT,this);
        return zooKeeper;
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
