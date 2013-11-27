import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;


public class ZooKeeperTest {
	
	public static void main(String[] args) throws IOException {

		ZooKeeper zk = new ZooKeeper("localhost:2181", 20, null);
		try {
			zk.exists("non-existing-path", false);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
