import org.apache.zookeeper.*;
import org.junit.Test;

import java.io.IOException;

public class ZookeeperOprationz {
    @Test
    public void Test() throws Exception {
        String connectString = "bigdata1:2181,bigdata2:2181,bigdata3:2181";

        int sessionTimeout = 1000;

        Watcher watcher = new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println("show me");
            }
        };
        ZooKeeper zooKeeper = new ZooKeeper(connectString,sessionTimeout, watcher);
//        final String path = "/chengxubin/test";
//        zooKeeper.create("/chengxubin", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,  CreateMode.PERSISTENT);

//        zooKeeper.setData("/zk", "/zk-1".getBytes(), -1);
        zooKeeper.delete("/zk", -1);

        zooKeeper.close();
    }
}
