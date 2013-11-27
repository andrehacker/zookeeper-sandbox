import org.apache.zookeeper.data.Stat;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.RetrySleeper;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;


public class CuratorTest {

	public static void main(String[] args) {
		CuratorFramework curator = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new RetryPolicy() {
			@Override
			public boolean allowRetry(int retryCount, long elapsedTimeMs,
					RetrySleeper sleeper) {
				System.out.println("allowRetry?");
				return false;
			}
		});
		curator.start();
		System.out.println("State after starting: " + curator.getState().toString()); 
		
		try {
			String parentNode = "/andre2";
			curator.create().forPath(parentNode, new byte[2]);
			Stat stat = curator.checkExists().forPath(parentNode);
			System.out.println("Data length: " + stat.getDataLength());
			curator.delete().forPath(parentNode);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		curator.close();
	}
	
}
