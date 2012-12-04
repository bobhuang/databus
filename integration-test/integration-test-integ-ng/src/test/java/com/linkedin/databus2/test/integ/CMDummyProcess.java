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

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.StateMachineEngine;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelFactory;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class CMDummyProcess
{

  public static final String zkServer = "zkSvr";
  public static final String cluster = "cluster";
  public static final String hostAddress = "host";
  public static final String hostPort = "port";
  public static final String relayCluster = "relayCluster";
  public static final String help = "help";
  public static final String configFile = "configFile";

  private final String zkConnectString;
  private final String clusterName;
  private final String instanceName;
  private HelixManager manager;
  private DummyStateModelFactory stateModelFactory;
  private StateMachineEngine genericStateMachineHandler;

  private String _file = null;

  public static final Logger LOG = Logger.getLogger(CMDummyProcess.class);

  public CMDummyProcess(String zkConnectString, String clusterName,
      String instanceName, String file)
  {
    this.zkConnectString = zkConnectString;
    this.clusterName = clusterName;
    this.instanceName = instanceName;
    this._file = file;
  }

  public void start() throws Exception
  {
    if (_file == null)
      manager = HelixManagerFactory.getZKHelixManager(
          clusterName, instanceName, InstanceType.PARTICIPANT, zkConnectString);
    else
    {
    	// no support for file based
    }
    	

    manager.connect();
    stateModelFactory = new DummyStateModelFactory();
    genericStateMachineHandler = manager.getStateMachineEngine();
    genericStateMachineHandler.registerStateModelFactory("OnlineOffline", stateModelFactory);
    genericStateMachineHandler.registerStateModelFactory("LeaderStandby", stateModelFactory);

    manager.getMessagingService().registerMessageHandlerFactory(genericStateMachineHandler.getMessageType(), genericStateMachineHandler);

    if (_file != null)
    {
      ClusterStateVerifier.verifyFileBasedClusterStates(_file, instanceName,
          stateModelFactory);

    }
  }

  public static class DummyStateModelFactory extends StateModelFactory
  {
    @Override
    public StateModel createNewStateModel(String stateUnitKey)
    {
      return new DummyStateModel();
    }

  }

  public static class DummyStateModel extends StateModel
  {

    public void onBecomeOfflineFromOnline(Message message,
        NotificationContext context)
    {
      System.out.println("DummyStateModel.onBecomeOfflineFromOnline()");

    }

    public void onBecomeOnlineFromOffline(Message message,
        NotificationContext context)
    {
      System.out.println("DummyStateModel.onBecomeOnlineFromOffline()");

    }
  }

 
  public static void main(String[] args) throws Exception
  {
    String zkConnectString;
    String clusterName;
    String instanceName;
    
    String file = null;

    if (args.length > 0)
    {
      CommandLine cmd = processCommandLineArgs(args);
      zkConnectString = cmd.getOptionValue(zkServer);
      clusterName = cmd.getOptionValue(cluster);

      String host = cmd.getOptionValue(hostAddress);
      String portString = cmd.getOptionValue(hostPort);
      int port = Integer.parseInt(portString);
      instanceName = host + "_" + port;

      file = cmd.getOptionValue(configFile);
      if (file != null)
      {
        File f = new File(file);
        if (!f.exists())
        {
          System.err.println("static config file doesn't exist");
          System.exit(1);
        }
      }
      // dbus2_driver.py will consume this
      System.out.println("Dummy process started");

      CMDummyProcess process = new CMDummyProcess(zkConnectString, clusterName,
          instanceName, file);

      process.start();
      Thread.currentThread().join();
    }
    else {
    	System.err.println("Error : Did not see any arguments passed in for zkConnectStringm clusterName and instanceName");
    	System.exit(1);
    	return;
    }
  }
  
  public static Thread start(final String[] args)
  {
	LOG.info("Starting DummyProcess");
    Thread t = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          CMDummyProcess.main(args);
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
    return t;
  }

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions()
  {
    Option helpOption = OptionBuilder.withLongOpt(help)
        .withDescription("Prints command-line options info").create();

    Option zkServerOption = OptionBuilder.withLongOpt(zkServer)
        .withDescription("Provide zookeeper address").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option clusterOption = OptionBuilder.withLongOpt(cluster)
        .withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Cluster name (Required)");

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
        .withDescription("Provide file to read states/messages").create();
    fileOption.setArgs(1);
    fileOption.setRequired(true);
    fileOption.setArgName("File to read states/messages (Optional)");

    OptionGroup optionGroup = new OptionGroup();
    optionGroup.addOption(zkServerOption);
    optionGroup.addOption(fileOption);

    Options options = new Options();
    options.addOption(helpOption);
    // options.addOption(zkServerOption);
    options.addOption(clusterOption);
    options.addOption(hostOption);
    options.addOption(portOption);

    options.addOptionGroup(optionGroup);

    return options;
  }

  private static void printUsage(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + ClusterSetup.class.getName(), cliOptions);
  }

  private static CommandLine processCommandLineArgs(String[] cliArgs)
      throws Exception
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();

    try
    {
      return cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe)
    {
      System.err
          .println("CommandLineClient: failed to parse command-line options: "
              + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }

  
}
