import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;


public class ZooKeeperRunner {
  
  private static final Logger LOG = Logger.getLogger(ZooKeeperRunner.class);
  
  private static final String DEFAULT_ZOOKEEPER_TICK_TIME = "2000";
  private static final String DEFAULT_ZOOKEEPER_INIT_LIMIT = "10";
  private static final String DEFAULT_ZOOKEEPER_SYNC_LIMIT = "5";
  
  private String zkDataDir = "";
  private String configFilePath = "";
  private String zkFullJarPath = "";
  
  /** ZooKeeper process */
  private Process zkProcess = null;
  private StreamCollector zkProcessCollector = null;
  
  public ZooKeeperRunner() {
    String workingDir = Paths.get("").toAbsolutePath().toString();
    this.zkDataDir = workingDir + "/zookeeper/data";
    this.configFilePath = workingDir + "/zoo.cfg";
    zkFullJarPath = workingDir + "/zookeeper-3.4.5.jar";
  }
  
  public static void main(String[] args) {
    ZooKeeperRunner runner = new ZooKeeperRunner();
    runner.startServer();

    String connectString = "127.0.0.1:2181";
    String nameSpace = "stratosphere";
    CuratorFramework curator = CuratorFrameworkFactory.builder().namespace(nameSpace).connectString(connectString).retryPolicy(new RetryPolicy() {
      @Override
      public boolean allowRetry(int arg0, long arg1, RetrySleeper arg2) {
      System.out.println("allowRetry? no!");
      return false;
      }
    }).build();
    curator.start();
    System.out.println("State after starting: " + curator.getState().toString());
    
    while(true) {
      System.out.println("> ");
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      try {
        String input = br.readLine();
        if ("exit".equals(input.toLowerCase())) {
          break;
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    System.out.println("Shut down program");
  }
  
  public void startServer() {
    
    File zkDirFile = new File(this.zkDataDir);
    try {
      System.out.println("onlineZooKeeperServers: Trying to delete old directory " + this.zkDataDir);
      FileUtils.deleteDirectory(zkDirFile);
    } catch (IOException e) {
      System.out.println("onlineZooKeeperServers: Failed to delete " +
          "directory " + this.zkDataDir + "exception" + e.toString());
    }
    
    generateZooKeeperConfigFile();
    
    ProcessBuilder processBuilder = new ProcessBuilder();
    List<String> commandList = Lists.newArrayList();
    String javaHome = System.getProperty("java.home");
    System.out.println("Java Home: " + javaHome);
    if (javaHome == null) {
      throw new IllegalArgumentException(
          "onlineZooKeeperServers: java.home is not set!");
    }
    commandList.add(javaHome + "/bin/java");
    String zkJavaOptsString = "-Xmx256m -XX:ParallelGCThreads=4 -XX:+UseConcMarkSweepGC " +
        "-XX:CMSInitiatingOccupancyFraction=70 -XX:MaxGCPauseMillis=100";
    String[] zkJavaOptsArray = zkJavaOptsString.split(" ");
    if (zkJavaOptsArray != null) {
      commandList.addAll(Arrays.asList(zkJavaOptsArray));
    }
    // Crash: jsp-api-2.1-6.1.14.jar:/home/andre/.m2/repository/org/mortbay/jetty/servlet-api-2.5/6.1.14/servlet-api-2.5-6.1.14.jar
    commandList.add("-cp");
    Path fullJarPath = new Path(this.zkFullJarPath);
    // System.getProperty("java.class.path")
    commandList.add(fullJarPath.toString() + ":/home/andre/dev/zookeeper/zookeeper-3.4.5/lib/*");
    commandList.add(QuorumPeerMain.class.getName());
    commandList.add(configFilePath);
    processBuilder.command(commandList);
    File execDirectory = new File(zkDataDir);
    processBuilder.directory(execDirectory);
    processBuilder.redirectErrorStream(true);
    LOG.info("onlineZooKeeperServers: Attempting to " +
        "start ZooKeeper server with command " + commandList +
        " in directory " + execDirectory.toString());
    try {
      synchronized (this) {
        System.out.println("START ME!");
        zkProcess = processBuilder.start();
        zkProcessCollector = new StreamCollector(zkProcess.getInputStream());
        zkProcessCollector.start();
      }
      Runnable runnable = new Runnable() {
        public void run() {
          LOG.info("run: Shutdown hook started.");
          synchronized (this) {
            if (zkProcess != null) {
              LOG.warn("onlineZooKeeperServers: " +
                       "Forced a shutdown hook kill of the " +
                       "ZooKeeper process.");
              zkProcess.destroy();
              int exitCode = -1;
              try {
                exitCode = zkProcess.waitFor();
              } catch (InterruptedException e) {
                LOG.warn("run: Couldn't get exit code.");
              }
              LOG.info("onlineZooKeeperServers: ZooKeeper process exited " +
                  "with " + exitCode + " (note that 143 " +
                  "typically means killed).");
            }
          }
        }
      };
      Runtime.getRuntime().addShutdownHook(new Thread(runnable));
      LOG.info("onlineZooKeeperServers: Shutdown hook added.");
    } catch (IOException e) {
      LOG.error("onlineZooKeeperServers: Failed to start " +
          "ZooKeeper process", e);
      throw new RuntimeException(e);
    }
//    zkProcessCollector.dumpLastLines(Level.ALL);

    // Once the server is up and running, notify that this server is up
    // and running by dropping a ready stamp.
//    int connectAttempts = 0;
//    final int maxConnectAttempts =
//        conf.getZookeeperConnectionAttempts();
//    while (connectAttempts < maxConnectAttempts) {
//      try {
//        if (LOG.isInfoEnabled()) {
//          LOG.info("onlineZooKeeperServers: Connect attempt " +
//              connectAttempts + " of " +
//              maxConnectAttempts +
//              " max trying to connect to " +
//              myHostname + ":" + zkBasePort +
//              " with poll msecs = " + pollMsecs);
//        }
//        InetSocketAddress zkServerAddress =
//            new InetSocketAddress(myHostname, zkBasePort);
//        Socket testServerSock = new Socket();
//        testServerSock.connect(zkServerAddress, 5000);
//        if (LOG.isInfoEnabled()) {
//          LOG.info("onlineZooKeeperServers: Connected to " +
//              zkServerAddress + "!");
//        }
//        break;
//      } catch (SocketTimeoutException e) {
//        LOG.warn("onlineZooKeeperServers: Got " +
//            "SocketTimeoutException", e);
//      } catch (ConnectException e) {
//        LOG.warn("onlineZooKeeperServers: Got " +
//            "ConnectException", e);
//      } catch (IOException e) {
//        LOG.warn("onlineZooKeeperServers: Got " +
//            "IOException", e);
//      }
//
//      ++connectAttempts;
//      try {
//        Thread.sleep(pollMsecs);
//      } catch (InterruptedException e) {
//        LOG.warn("onlineZooKeeperServers: Sleep of " + pollMsecs +
//            " interrupted - " + e.getMessage());
//      }
//    }
//    if (connectAttempts == maxConnectAttempts) {
//      throw new IllegalStateException(
//          "onlineZooKeeperServers: Failed to connect in " +
//              connectAttempts + " tries!");
//    }
//    Path myReadyPath = new Path(
//        serverDirectory, myHostname +
//        HOSTNAME_TASK_SEPARATOR + taskPartition);
//    try {
//      if (LOG.isInfoEnabled()) {
//        LOG.info("onlineZooKeeperServers: Creating my filestamp " +
//            myReadyPath);
//      }
//      fs.createNewFile(myReadyPath);
//    } catch (IOException e) {
//      LOG.error("onlineZooKeeperServers: Failed (maybe previous " +
//          "task failed) to create filestamp " + myReadyPath, e);
//    }

  }
  
  // List<String> serverList
  private void generateZooKeeperConfigFile() {
    
//    String zkDir = System.getProperty("user.dir");
    String zkClientPort = "2181";
    System.out.println("ZooKeeper data dir: " + zkDataDir);
    System.out.println("ZooKeeper config file: " + this.configFilePath);
    try {
      File zkDirFile = new File(zkDataDir);
      boolean mkDirRet = zkDirFile.mkdirs();
      File configFile = new File(this.configFilePath);
      boolean deletedRet = configFile.delete();
      if (!configFile.createNewFile()) {
        throw new IllegalStateException(
            "generateZooKeeperConfigFile: Failed to " +
                "create config file " + configFile.getName());
      }
      // Make writable by everybody
      if (!configFile.setWritable(true, false)) {
        throw new IllegalStateException(
            "generateZooKeeperConfigFile: Failed to make writable " +
                configFile.getName());
      }

      Writer writer = null;
      try {
        writer = new FileWriter(configFilePath);
        writer.write("tickTime=" +
            DEFAULT_ZOOKEEPER_TICK_TIME + "\n");
        writer.write("dataDir=" + zkDataDir + "\n");
        writer.write("clientPort=" + zkClientPort + "\n");
//        writer.write("maxClientCnxns=" +
//            GiraphConstants.DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS +
//            "\n");
//        writer.write("minSessionTimeout=" +
//            conf.getZooKeeperMinSessionTimeout() + "\n");
//        writer.write("maxSessionTimeout=" +
//            conf.getZooKeeperMaxSessionTimeout() + "\n");
        writer.write("initLimit=" +
            DEFAULT_ZOOKEEPER_INIT_LIMIT + "\n");
        writer.write("syncLimit=" +
            DEFAULT_ZOOKEEPER_SYNC_LIMIT + "\n");
//        writer.write("snapCount=" +
//            GiraphConstants.DEFAULT_ZOOKEEPER_SNAP_COUNT + "\n");
//        writer.write("forceSync=" +
//            (conf.getZooKeeperForceSync() ? "yes" : "no") + "\n");
//        writer.write("skipACL=yes\n"); // or no
//        if (serverList.size() != 1) {
//          writer.write("electionAlg=0\n");
//          for (int i = 0; i < serverList.size(); ++i) {
//            writer.write("server." + i + "=" + serverList.get(i) +
//                ":" + (zkClientPort + 1) +
//                ":" + (zkClientPort + 2) + "\n");
//            if (myHostname.equals(serverList.get(i))) {
//              Writer myidWriter = null;
//              try {
//                myidWriter = new FileWriter(zkDataDir + "/myid");
//                myidWriter.write(i + "\n");
//              } finally {
//                Closeables.closeQuietly(myidWriter);
//              }
//            }
//          }
//        }
      } finally {
        Closeables.closeQuietly(writer);
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "generateZooKeeperConfigFile: Failed to write file", e);
    }
  }
  
  
  
  /**
   * Collects the output of a stream and dumps it to the log.
   */
  private static class StreamCollector extends Thread {
    /** Number of last lines to keep */
    private static final int LAST_LINES_COUNT = 100;
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(StreamCollector.class);
    /** Buffered reader of input stream */
    private final BufferedReader bufferedReader;
    /** Last lines (help to debug failures) */
    private final LinkedList<String> lastLines = Lists.newLinkedList();
    /**
     * Constructor.
     *
     * @param is InputStream to dump to LOG.info
     */
    public StreamCollector(final InputStream is) {
      super(StreamCollector.class.getName());
      setDaemon(true);
      InputStreamReader streamReader = new InputStreamReader(is,
          Charset.defaultCharset());
      bufferedReader = new BufferedReader(streamReader);
    }

    @Override
    public void run() {
      readLines();
    }

    /**
     * Read all the lines from the bufferedReader.
     */
    private synchronized void readLines() {
      String line;
      try {
        while ((line = bufferedReader.readLine()) != null) {
          if (lastLines.size() > LAST_LINES_COUNT) {
            lastLines.removeFirst();
          }
          lastLines.add(line);

          if (LOG.isDebugEnabled()) {
            LOG.debug("readLines: " + line);
          }
        }
      } catch (IOException e) {
        LOG.error("readLines: Ignoring IOException", e);
      }
    }

    /**
     * Dump the last n lines of the collector.  Likely used in
     * the case of failure.
     *
     * @param level Log level to dump with
     */
    public synchronized void dumpLastLines(Level level) {
      // Get any remaining lines
      readLines();
      // Dump the lines to the screen
      for (String line : lastLines) {
        LOG.log(level, line);
      }
    }
  }

}
