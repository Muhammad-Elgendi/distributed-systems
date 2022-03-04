import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.List;

public class WatcherDemo implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;

    // Znode to be watched
    private static final String TargetZnode = "/target_znode";


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        WatcherDemo watcherDemo = new WatcherDemo();
        watcherDemo.connectToZookeeper();

        // display data about target znode and register the watcher for the first time
        watcherDemo.watchTargetZnode();

        // make main thread waits
        watcherDemo.run();

        watcherDemo.close();
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

    // Register our watcher
    public void watchTargetZnode() throws InterruptedException, KeeperException {
        // watch creation and deletion of the target Znode
        Stat stat = zooKeeper.exists(TargetZnode,this);

        // if The Znode doesn't exist yet, it returns Null
        if(stat == null){
            return;
        }

        // if Znode exists
        // register for data changes and modifications
        byte [] data = zooKeeper.getData(TargetZnode,this,stat);
        // register for children changes
        List<String> children = zooKeeper.getChildren(TargetZnode,this);

        System.out.println("Data : "+new String(data)+" children : "+children);

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

            // handle deletion of target node
            case NodeDeleted:
                System.out.println(TargetZnode+" was deleted");
                break;

            // handle creation of target node
            case NodeCreated:
                System.out.println(TargetZnode+" was created");
                break;

            // handle data changes of the target znode
            case NodeDataChanged:
                System.out.println(TargetZnode+" data changed");
                break;

            // handle children changes of target znode
            case NodeChildrenChanged:
                System.out.println(TargetZnode+" children changed");
                break;
        }

        // Get all up-to-date data after changes AND RE-register our watcher for future events
        try {
            watchTargetZnode();
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        }

    }
}
