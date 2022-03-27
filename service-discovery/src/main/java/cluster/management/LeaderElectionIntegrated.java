package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

public class LeaderElectionIntegrated implements Watcher {
    private static final String ELECTION_NAMESPACE = "/election";
    private ZooKeeper zooKeeper;
    private String currentZnodeName;
    private final OnElectionCAllback onElectionCAllback;

    public LeaderElectionIntegrated(ZooKeeper zooKeeper,OnElectionCAllback onElectionCAllback) {
        this.zooKeeper = zooKeeper;
        this.onElectionCAllback = onElectionCAllback;
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix,
                new byte[]{},
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reelectLeader() throws KeeperException, InterruptedException {
        Stat predecessorStat = null;
        String predecessorZnodeName = "";
        while (predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader");
                this.onElectionCAllback.onElectedToBeLeader();
                return;
            } else {
                System.out.println("I am not the leader");
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }

        this.onElectionCAllback.onWorker();
        System.out.println("Watching znode " + predecessorZnodeName);
        System.out.println();
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeDeleted:
                try {
                    reelectLeader();
                } catch (InterruptedException e) {
                } catch (KeeperException e) {
                }
        }
    }

}