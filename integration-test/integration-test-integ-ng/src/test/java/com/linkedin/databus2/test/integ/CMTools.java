/**
 * 
 */
package com.linkedin.databus2.test.integ;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.databus2.test.integ.CMZooKeeperServer;

/**
 * @author pganti
 *
 */
public class CMTools {

	public static final Logger LOG = Logger.getLogger(CMTools.class);

	final static String _zkSvrStartCmdTemplate = "-zkSvr localhost:%d ";
	final static String _zkSvrRemoteStartCmdTemplate = "-zkSvr %s:%d ";

	/**
	 * 
	 * @param args
	 */
	public static Thread startClusterManager(final int zkPort, final String relayClusterName)	{
		Thread t = new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				HelixManager cmgr = null;
				try
				{
					String zkConnectString = "localhost:" + zkPort;
					cmgr = HelixControllerMain.startHelixController(zkConnectString, relayClusterName, "ShiMagic", "STANDALONE");
					Thread.currentThread().join();
				} catch (Exception e)
				{
					LOG.info("Cluster Manager main could not be started " + e.getMessage());
				} finally 
				{
					if (cmgr != null)
						cmgr.disconnect();
				}
				
			}
		});
		t.start();
		return t;
	}

	/**
	 * 
	 * @param zkPort
	 * @param relayClusterName
	 * @param relayPort
	 */
	public static void startDummyProcess(int zkPort, String relayClusterName, int relayPort) {
		LOG.info("Starting Dummy Process");
		final String zkSvrStartCmd = String.format(_zkSvrStartCmdTemplate, zkPort);		
		final String args[] = createArgs(zkSvrStartCmd + "-cluster " + relayClusterName + " -host localhost -port " +Integer.toString(relayPort));
		CMDummyProcess.start(args);
	}
	
	/**
	 * 
	 * @param zkPort
	 * @param relayClusterName
	 * @return
	 */
    public static boolean verifyState(int zkPort, String relayClusterName) {
		final String zkSvrStartCmd = String.format(_zkSvrStartCmdTemplate, zkPort);
        boolean state = ClusterStateVerifier.verifyState(createArgs(zkSvrStartCmd + "-cluster " + relayClusterName));
		LOG.info("Verified state " + Boolean.toString(state));
		return state;

    }


	/**
	 * 
	 * @param zkPort
	 * @param relayClusterName
	 */
	public static void addCluster(int zkPort, String relayClusterName) 
	throws Exception {
                try {
			final String zkSvrStartCmd = String.format(_zkSvrStartCmdTemplate, zkPort);
			ClusterSetup.processCommandLineArgs(createArgs(zkSvrStartCmd + "-addCluster " + relayClusterName));
		} catch (Exception e)
		{
			LOG.info("Could not add model. It probably exists already");
		}
		return;
	}

	/**
	 * 
	 * @param zkPort
	 * @param relayClusterName
	 * @param nodeName
	 */
	public static void addNode(int zkPort, String relayClusterName, String nodeName) 
	throws Exception {
		try {
		final String zkSvrStartCmd = String.format(_zkSvrStartCmdTemplate, zkPort);		
		ClusterSetup.processCommandLineArgs(createArgs(zkSvrStartCmd + "-addNode " + relayClusterName + " " + nodeName ));
		} catch (Exception e){
			LOG.info("Could not add node, it probably already exists" + e.getMessage());
		}
	}

	/**
	 * 
	 * @param zkPort
	 * @param relayClusterName
	 * @param nodeName
	 */
	public static void addRemoteNode(String zkHost, int zkPort, String clusterName, String nodeName) 
	throws Exception {
		final String zkSvrStartCmd = String.format(_zkSvrRemoteStartCmdTemplate, zkHost, zkPort);		
		ClusterSetup.processCommandLineArgs(createArgs(zkSvrStartCmd + "-addNode " + clusterName + " " + nodeName ));
	}
	
	/**
	 * 
	 * @param zkPort
	 * @param relayClusterName
	 * @param nodeName
	 * @throws Exception
	 */
	public static void addResourceGroup(int zkPort, String relayClusterName, String databaseName, int numPartitions, String stateModel) 
	throws Exception {
		final String zkSvrStartCmd = String.format(_zkSvrStartCmdTemplate, zkPort);
		ClusterSetup.processCommandLineArgs(createArgs(zkSvrStartCmd + "-addResource " + relayClusterName + " " + databaseName + " " + numPartitions + " " + stateModel));
	}

	/**
	 * 
	 * @param zkPort
	 * @param relayClusterName
	 * @param databaseName
	 * @throws Exception
	 */
	public static void rebalance(int zkPort, String relayClusterName, String databaseName, int replicationFactor) 
	throws Exception {
		final String zkSvrStartCmd = String.format(_zkSvrStartCmdTemplate, zkPort);
		ClusterSetup.processCommandLineArgs(createArgs(zkSvrStartCmd + "-rebalance " + relayClusterName + " " + databaseName + " " + replicationFactor));
	}

	/**
	 * 
	 * @param zkPort
	 * @param relayClusterName
	 * @throws Exception
	 */
	public static ZNRecord generateRelayIdealState(int zkPort, 
			String relayClusterName, 
			CMZooKeeperServer cmZk,
			List<String> storageNodes)
	throws Exception {
		
		ZNRecord zr = generateIdealState(storageNodes);
		
		writeIdealStateToClusterManager(zkPort, relayClusterName, zr, cmZk);
		return zr;
	}

	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	private static ZNRecord generateIdealState(List<String> storageNodes)
	throws Exception {
		  /**
		   *  partition, physical node information
		   */
		  final int NUM_PARTITIONS     = 1;
		  final int REPLICATION_FACTOR = 1;
		  final int SCHEMA_VERSION = 1;


		  final String ESPRESSO_DBNAME = "BizProfile";		  
		  final String physicalPartitionName = ESPRESSO_DBNAME;
		  final String logicalPartitionName  = "p%d_" + SCHEMA_VERSION;
		  
		  /**
		   *  Physical Relay machines
		   */
		  final List<String> relayNodes = new ArrayList<String>(
				  Arrays.asList(
				  "localhost_10015"
				  ));
		  
		  MockIdealStateGenerator risg = new MockIdealStateGenerator();
		  ZNRecord zr = risg.espressoRelayIdealStateGenerator(NUM_PARTITIONS, REPLICATION_FACTOR, 
				  											  relayNodes, storageNodes, 
				  											  physicalPartitionName, logicalPartitionName, 
				  											  ESPRESSO_DBNAME);
		
		return zr;
	}

	private static boolean writeIdealStateToClusterManager(int zkPort, String relayClusterName, ZNRecord zr, CMZooKeeperServer cmZk)
	throws Exception {
		
		ZkClient client = cmZk.getZkClient();
		client.setZkSerializer(new ZNRecordSerializer());
		ZKDataAccessor cda = new ZKDataAccessor(relayClusterName, client);
		cda.setProperty(com.linkedin.helix.PropertyType.IDEALSTATES, zr, zr.getId());		
		return true;
	}
	
	
	
	/**
	 * 
	 * @param str
	 * @return
	 */
	private static String[] createArgs(String str)
	{
		String[] split = str.split("[ ]+");
		System.out.println(Arrays.toString(split));
		return split;
	}
}
