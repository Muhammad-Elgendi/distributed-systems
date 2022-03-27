import cluster.management.OnElectionCAllback;
import cluster.management.ServiceRegistryIntegrated;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCAllback {

    private final ServiceRegistryIntegrated serviceRegistry;
    private final int port;

    public OnElectionAction(ServiceRegistryIntegrated serviceRegistry, int port) {
        this.serviceRegistry = serviceRegistry;
        this.port = port;
    }

    @Override
    public void onElectedToBeLeader() {
        try {
            serviceRegistry.unregisterFromCluster();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        serviceRegistry.registerForUpdates();
    }

    @Override
    public void onWorker() {
        try {

            String currentServerAddress = String.format("http://%s:%d",
                    InetAddress.getLocalHost().getCanonicalHostName(),
                    port
            );

            serviceRegistry.registerToCluster(currentServerAddress);

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
