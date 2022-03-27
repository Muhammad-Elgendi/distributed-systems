package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistryIntegrated implements Watcher {
        private static final String REGISTRY_ZNODE = "/service_registry";
        private final ZooKeeper zooKeeper;

        private String currentZnode = null;

        private List<String> allServiceAddresses = null;

    public ServiceRegistryIntegrated(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;

        // create REGISTRY_ZNODE as a PERSISTENT node
        createServiceRegistryZnode();
    }

    public void createServiceRegistryZnode(){
        // check if REGISTRY_ZNODE is existed in the zookeeper
        // if not, we create it as a PERSISTENT node

        // Race condition may happen here but zookeeper will handle it. How? check your notes.
        try {
            if(zooKeeper.exists(REGISTRY_ZNODE,false) == null){
                zooKeeper.create(REGISTRY_ZNODE,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void registerToCluster(String metaData) throws InterruptedException, KeeperException {

        // by making it EPHEMERAL_SEQUENTIAL,
        // we simply avoid any name collisions but the actual sequence number is not important for us at this time
        this.currentZnode = zooKeeper.create(REGISTRY_ZNODE + "/n_" ,metaData.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("Registered with the service registry as "+this.currentZnode);
    }

    // joining and leaving the cluster
    // So let's create the synchronized updateAddresses() method
    // which will be called potentially by multiple threads.
    private synchronized void updateAddresses() throws InterruptedException, KeeperException {

        // get all nodes from zookeeper
        // and also register for any changes in that list to get notifications
        List<String> workerNodes = zooKeeper.getChildren(REGISTRY_ZNODE,this);

        // a temporary list to store all the cluster's addresses
        ArrayList<String> addresses = new ArrayList<>(workerNodes.size());

        for (String workerNode: workerNodes) {

            // construct the full path for each znode
            String nodeFullPath = REGISTRY_ZNODE + "/" + workerNode;

            // then call the exists() method on that child znode to get its stats
            // as a prerequisite for getting the znode's data.
            Stat stat = zooKeeper.exists(nodeFullPath,false);

            // If between getting the list of children and calling the exists() method that child znode disappears.
            // Then there are result from the method is going to be null.
            // This is yet another race condition that we have no control over but we handle it by simply continuing
            // to the next znode in the list.
            if(stat == null)
                continue;

            // if znode does exist
            byte[] addressBytes = zooKeeper.getData(nodeFullPath,false,stat);
            String address = new String(addressBytes);
            addresses.add(address);
        }

        // we will wrap that list with an unmodifiable
        // list object and store it in the all service addresses member variable
        // because the method is synchronized
        // this entire update will happen atomically
        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The cluster addresses are : " + this.allServiceAddresses);
    }

    // we registered for updates about any changes in the cluster.
    // So let's handle those changes events inside the process() method.
    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            // Again this will update our all service addresses variable and reregister us for future updates.
            updateAddresses();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    // The one thing we're missing right now is the initial call to updateAddresses()
    public void registerForUpdates(){
        try {
            updateAddresses();
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        }
    }

    // we may also want to get the list of the most up-to-date addresses.
    // So let's create the synchronized get all service addresses method which will give us those cached results.
    public synchronized List<String> getAllServiceAddresses() throws InterruptedException, KeeperException {

        // If the allServiceAddresses is null then the caller simply forgot to call registerForUpdates()
        // so to make it safe we update it for them
        if(this.allServiceAddresses == null){
            updateAddresses();
        }

        return this.allServiceAddresses;
    }

    // The last feature we need to add is the ability to unregister from the cluster.
    // This is very useful if the node is gracefully shut down itself or if the worker suddenly becomes a leader
    // so, it would want to unregister to avoid communicating with itself.
    public void unregisterFromCluster() throws InterruptedException, KeeperException {

        // first check that we indeed have
        // an existent znode registered with the service registry
        if(currentZnode!= null && zooKeeper.exists(currentZnode,false) != null){
            zooKeeper.delete(currentZnode,-1);
        }
    }
}
