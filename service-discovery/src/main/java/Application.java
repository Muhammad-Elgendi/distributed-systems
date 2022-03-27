import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException {

        Application application = new Application();
        application.connectToZookeeper();

        // make main thread waits
        application.run();

        application.close();
        System.out.println("Disconnected from Zookeeper");
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
                if(watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected){
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
