import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class ZooKeeperTest {
	
	public static void main(String[] args) throws IOException {

		// Comma separated list of host:port. Connects to any of them.
		// Timeout = min 2x, max 20x tickTime from config
		// Creates a session = 64 bit number
		ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 200, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println("Default Watcher event: " + event.getType());
			}
		});
		
		try {
			
			// READ: getData(), getChildren(), exists()
			
//			zk.create("/andre", new byte[10], null, CreateMode.PERSISTENT);
			String path = "/andre";
			Stat stat = zk.exists("/andre", false);
			System.out.println("Path '" + path + "' exists: " + stat);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		// create, exists, delete, getData, setData, getChildren
	}

}
