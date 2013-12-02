import java.nio.ByteBuffer;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.zookeeper.data.Stat;



public class CuratorTest {

	public static void main(String[] args) {
	  
	  String connectString = "127.0.0.1:2181";
	  String nameSpace = "stratosphere";
	  
	  CuratorFramework curator = CuratorFrameworkFactory.builder().namespace(nameSpace).connectString(connectString).retryPolicy(new RetryPolicy() {
      @Override
      public boolean allowRetry(int arg0, long arg1, RetrySleeper arg2) {
      System.out.println("allowRetry?");
      return false;
      }
    }).build();
//		CuratorFramework curator = CuratorFrameworkFactory.newClient(connectString, new RetryPolicy() {
//			@Override
//			public boolean allowRetry(int retryCount, long elapsedTimeMs,
//					RetrySleeper sleeper) {
//				System.out.println("allowRetry?");
//				return false;
//			}
//		});
		curator.start();
		System.out.println("State after starting: " + curator.getState().toString());
		
		int counterInt = 12345;
		double counterDouble = 12345.2123;
		try {
		  
		  // ------- MANUAL COUNTER ------
		  String jobId = "123456789";
		  String jobNode = "/" + jobId;
		  String counterIntNode = jobNode + "/" + "counterInt";
		  String counterDoubleNode = jobNode + "/" + "counterDouble";
		  
//      curator.delete().deletingChildrenIfNeeded().forPath(jobNode);
      
			curator.create().forPath(jobNode);
			curator.create().forPath(counterIntNode, toBytes(counterInt));
			curator.create().forPath(counterDoubleNode, toBytes(counterDouble));
			
			Stat stat = curator.checkExists().forPath(jobNode);
			System.out.println("Data length: " + stat.getDataLength());
			
			byte[] data = curator.getData().forPath(counterIntNode);
			System.out.println("Data: " + bytesToInt(data));

			byte[] data2 = curator.getData().forPath(counterDoubleNode);
      System.out.println("Data: " + bytesToDouble(data2));
			
      curator.delete().deletingChildrenIfNeeded().forPath(jobNode);
      
      // ------- SHARED COUNTER RECIPE ------
      
      SharedCount sharedCount = new SharedCount(curator, "shared-count", 0);
      
      sharedCount.start();
      
      sharedCount.trySetCount(sharedCount.getCount());
      
      sharedCount.close();
      
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		curator.close();
	}
	
	private static int bytesToInt(byte[] data) {
	  return ByteBuffer.allocate(4).put(data).getInt(0);
	}

  private static double bytesToDouble(byte[] data) {
	    return ByteBuffer.allocate(8).put(data).getDouble(0);
	  }

	private static byte[] toBytes(int number) {
    return ByteBuffer.allocate(4).putInt(number).array();
	}

	 private static byte[] toBytes(double number) {
	    return ByteBuffer.allocate(8).putDouble(number).array();
	  }

}
