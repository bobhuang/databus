package com.linkedin.databus2.test.integ;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.databus3.espresso.cmclient.ClusterParams;
import com.linkedin.databus3.espresso.cmclient.RelayAdapter;

public class CMRelayProcess {
	
	private static Logger LOG = Logger.getLogger(CMRelayProcess.class);

	public static final String zkServer = "zkSvr";
	public static final String relayCluster = "relayCluster";
	public static final String storageCluster = "storageCluster";
	public static final String hostAddress = "host";
	public static final String hostPort = "port";
	public static final String help = "help";
	public static final String configFile = "configFile";

	private final ClusterParams relayClusterParams_, storageClusterParams_;
	private final String instanceName_;
	private String file_ = null;

	public CMRelayProcess(String instanceName, ClusterParams relayClusterParams,
						  ClusterParams storageClusterParams, String file) throws Exception {
		relayClusterParams_ = relayClusterParams;
		storageClusterParams_ = storageClusterParams;
		instanceName_ = instanceName;
		file_ = file;
	}

	/**
	 * Connects the relay process to the cluster manager and makes it a "live" instance
	 * @throws Exception
	 */
	public void start() throws Exception {
		RelayAdapter relayAdapter = new RelayAdapter(instanceName_, relayClusterParams_, storageClusterParams_, file_, 1);
		relayAdapter.connect();
	}

	public static void main(String[] args) throws Exception {
		LOG.info("From main() inside TestRelayProcess Main");
		String instanceName, file;

		if (args.length > 0) {
			CommandLine cmd = processCommandLineArgs(args);
			String relayZkConnectString = cmd.getOptionValue(zkServer);
			String relayClusterName = cmd.getOptionValue(relayCluster);
			String storageZkConnectString = relayZkConnectString;
			String storageClusterName = cmd.getOptionValue(storageCluster);
			
			ClusterParams relayClusterParams = new ClusterParams(relayClusterName, relayZkConnectString);
			ClusterParams storageClusterParams = new ClusterParams(storageClusterName, storageZkConnectString);
			
			String host = cmd.getOptionValue(hostAddress);
			String portString = cmd.getOptionValue(hostPort);
			int port = Integer.parseInt(portString);
			instanceName = host + "_" + port;

			file = cmd.getOptionValue(configFile);
			if (file != null) {
				File f = new File(file);
				if (!f.exists()) {
					System.err.println("static config file doesn't exist");
					System.exit(1);
				}
			}
			// dbus2_driver.py will consume this
			System.out.println("TestRelayProcess has been started");

			CMRelayProcess process = new CMRelayProcess(instanceName, relayClusterParams, 
														storageClusterParams, file);
			process.start();
			Thread.currentThread().join();
		} else {
			System.err
					.println("Error : Did not see any arguments passed in for zkConnectStringm clusterName and instanceName");
			System.exit(1);
			return;
		}
	}

	public static void start(final String[] args)
	{
		LOG.info("Starting Relay Process");
		Thread t = new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				try
				{
					CMRelayProcess.main(args);
				} catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		});
		t.start();
		try
		{
			Thread.sleep(1000);
		} catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static CommandLine processCommandLineArgs(String[] cliArgs)
			throws Exception {
		CommandLineParser cliParser = new GnuParser();
		Options cliOptions = constructCommandLineOptions();

		try {
			return cliParser.parse(cliOptions, cliArgs);
		} catch (ParseException pe) {
			System.err
					.println("CommandLineClient: failed to parse command-line options: "
							+ pe.toString());
			printUsage(cliOptions);
			System.exit(1);
		}
		return null;
	}

	private static void printUsage(Options cliOptions) {
		HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.printHelp("java " + ClusterSetup.class.getName(),
				cliOptions);
	}

	@SuppressWarnings("static-access")
	private static Options constructCommandLineOptions() {
		Option helpOption = OptionBuilder.withLongOpt(help)
				.withDescription("Prints command-line options info").create();

		Option zkServerOption = OptionBuilder.withLongOpt(zkServer)
				.withDescription("Provide zookeeper address").create();
		zkServerOption.setArgs(1);
		zkServerOption.setRequired(true);
		zkServerOption.setArgName("ZookeeperServerAddress(Required)");

		Option relayClusterOption = OptionBuilder.withLongOpt(relayCluster)
				.withDescription("Provide relay cluster name").create();
		relayClusterOption.setArgs(1);
		relayClusterOption.setRequired(true);
		relayClusterOption.setArgName("Cluster name (Required)");

		Option storageClusterOption = OptionBuilder.withLongOpt(storageCluster)
				.withDescription("Provide storage cluster name").create();
		storageClusterOption.setArgs(1);
		storageClusterOption.setRequired(true);
		storageClusterOption.setArgName("Storage Cluster name (Required)");

		Option hostOption = OptionBuilder.withLongOpt(hostAddress)
				.withDescription("Provide host name").create();
		hostOption.setArgs(1);
		hostOption.setRequired(true);
		hostOption.setArgName("Host name (Required)");

		Option portOption = OptionBuilder.withLongOpt(hostPort)
				.withDescription("Provide host port").create();
		portOption.setArgs(1);
		portOption.setRequired(true);
		portOption.setArgName("Host port (Required)");

		// add an option group including either --zkSvr or --configFile
		Option fileOption = OptionBuilder.withLongOpt(configFile)
				.withDescription("Provide file to read states/messages")
				.create();
		fileOption.setArgs(1);
		fileOption.setRequired(true);
		fileOption.setArgName("File to read states/messages (Optional)");

		OptionGroup optionGroup = new OptionGroup();
		optionGroup.addOption(zkServerOption);
		optionGroup.addOption(fileOption);

		Options options = new Options();
		options.addOption(helpOption);
		// options.addOption(zkServerOption);
		options.addOption(relayClusterOption);
		options.addOption(storageClusterOption);
		options.addOption(hostOption);
		options.addOption(portOption);

		options.addOptionGroup(optionGroup);

		return options;
	}


}
