package xo.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Client {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    private ZooKeeper zk;
    private final String groupName = "/servers";;

    private void getServerList() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(groupName, true);
        List<String> servers = new ArrayList<>();
        for (String child: children) {
            byte[] data = zk.getData(groupName + "/" + child, null, null);
            servers.add(new String(data));
        }
        LOG.info("--------------当前服务器列表--------------");
        for (String server: servers) {
            LOG.info(server);
        }
        LOG.info("----------------------------------------");
    }

    private void startListenServerListChange() throws IOException {
        String connectString = "localhost:2181";
        int sessionTimeout = 3000;
        this.zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    LOG.info("Received event: " + event.getType());
                    getServerList();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Client client= new Client();
        client.startListenServerListChange();
        Thread.sleep(30000);
    }
}
