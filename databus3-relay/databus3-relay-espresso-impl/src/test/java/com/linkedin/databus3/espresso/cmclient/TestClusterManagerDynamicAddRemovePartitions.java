package com.linkedin.databus3.espresso.cmclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.relay.AddRemovePartitionInterface;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;
import com.linkedin.databus3.espresso.schema.EspressoBackedSchemaRegistryService;
import com.linkedin.espresso.common.config.InvalidConfigException;
import com.linkedin.espresso.schema.SchemaRegistry;

@Test(singleThreaded=true)
public class TestClusterManagerDynamicAddRemovePartitions {
  public static final Logger LOG = Logger.getLogger(TestClusterManagerDynamicAddRemovePartitions.class);
  
  private DummyRelayObject _dummyRelay;
  private EspressoBackedSchemaRegistryService _schemaRegistry;
  private SourceIdNameRegistry _sourceIdNameRegistry;
  
  private final static String DB_NAME = "BizProfile"; //because we have to use a real registry - we have to use a real name :(
  private final static String SCHEMA_ROOTDIR_PROP_NAME = "espresso.schema.rootdir";
  private final static String DEFAULT_ESPRESSO_SCHEMA_ROOTDIR = "../databus3-relay-cmdline-pkg/espresso_schemas_registry";
  private static String SCHEMA_ROOTDIR;
  
  @BeforeTest
  public void setUp() throws InvalidConfigException, com.linkedin.databus.core.util.InvalidConfigException, DatabusException {
    _dummyRelay = new DummyRelayObject();
    
    SCHEMA_ROOTDIR = System.getProperty(SCHEMA_ROOTDIR_PROP_NAME, DEFAULT_ESPRESSO_SCHEMA_ROOTDIR);
    LOG.info("Using espresso schema registry root: " + SCHEMA_ROOTDIR);
    
    // create fake registries - we have to use a real espresso schema registry
    SchemaRegistry.Config configBuilder = new SchemaRegistry.Config();
    configBuilder.setMode("file");

    configBuilder.setRootSchemaNamespace(SCHEMA_ROOTDIR);

    SchemaRegistry.StaticConfig config = configBuilder.build();
    SourceIdNameRegistry sourceIdNameRegistry = new SourceIdNameRegistry();

    _schemaRegistry = new EspressoBackedSchemaRegistryService(config, sourceIdNameRegistry);
    List<IdNamePair> pairs = new ArrayList<IdNamePair>();
    long i = 0;
    for(String source :_schemaRegistry.getAllLogicalSourcesInDatabase(DB_NAME)) {
      pairs.add(new IdNamePair(i++, source));
    }
    _sourceIdNameRegistry = new SourceIdNameRegistry();
    _sourceIdNameRegistry.updateFromIdNamePairs(pairs);
  }
  
  // this class just counts number of time a partition identified by partition name and id 
  // was added/removed
  public static class DummyRelayObject implements AddRemovePartitionInterface {
    private Map<String, Integer> _addedPartitions = new HashMap<String, Integer>(20);
    private Map<String, Integer> _removedPartitions = new HashMap<String, Integer>(20);
    public boolean _dropped = false;
    
    @Override
    public void addPartition(PhysicalSourceStaticConfig pConfig) {
      addOneToMap(_addedPartitions, pConfig);
    }

    @Override
    public void removePartition(PhysicalSourceStaticConfig pConfig) {
      addOneToMap(_removedPartitions, pConfig);
    }
    
    @Override
    public void dropDatabase(String dbName) throws DatabusException {
    	_dropped = true;
    }
    
    private void addOneToMap(Map<String, Integer> map, PhysicalSourceStaticConfig pConfig) {
      String partId = pConfig.getName() + "_" + pConfig.getId();
      Integer val = map.get(partId);
      if(val == null) {
        val = new Integer(0);
      }
      map.put(partId, val+1);
    }
    
    public int getNumberAdded(String pPart) {
      return getNumber(_addedPartitions, pPart);
    }
    public int getNumberRemoved(String pPart) {
      return getNumber(_removedPartitions, pPart);
    }
    
    private int getNumber(Map<String, Integer> map, String pPart) {
      Integer val = map.get(pPart);
      if(val == null)
        return 0;
      else 
        return val.intValue();
    }
  }

  @Test
  public void addRemovePartitions() throws Exception {
    ClusterManagerPhysicalSourceConfigBuilder cmPSCB = 
        new ClusterManagerPhysicalSourceConfigBuilder(null, _sourceIdNameRegistry, _schemaRegistry, _dummyRelay); 
    ResourceManager rm  = new ResourceManager();
    rm.setListener(cmPSCB); // listener will be called on add/remove calls
    
    // add 1 partition for partId = 1
    rm.addResource("ela4-db1-espresso.prod.linkedin.com_1521,"+DB_NAME+",p1_1,MASTER");
    
    // add 2 partition for partId = 2
    rm.addResource("ela4-db1-espresso.prod.linkedin.com_1521,"+DB_NAME+",p2_1,MASTER");
    rm.addResource("ela4-db1-espresso.prod.linkedin.com_1521,"+DB_NAME+",p2_2,MASTER");

    // add 3 partition for partId = 3
    rm.addResource("ela4-db1-espresso.prod.linkedin.com_1521,"+DB_NAME+",p3_1,MASTER");
    rm.addResource("ela4-db1-espresso.prod.linkedin.com_1521,"+DB_NAME+",p3_2,MASTER");
    rm.addResource("ela4-db1-espresso.prod.linkedin.com_1521,"+DB_NAME+",p3_3,MASTER");

    // remove one with partId = 2
    rm.removeResource("ela4-db1-espresso.prod.linkedin.com_1521,"+DB_NAME+",p2_1,MASTER");
    
    // remove two with partId = 3
    rm.removeResource("ela4-db1-espresso.prod.linkedin.com_1521,"+DB_NAME+",p3_1,MASTER");
    rm.removeResource("ela4-db1-espresso.prod.linkedin.com_1521,"+DB_NAME+",p3_3,MASTER");
    
    // partId = 1; added = 1, removed = 0
    Assert.assertEquals(1, _dummyRelay.getNumberAdded(DB_NAME + "_1"));
    Assert.assertEquals(0, _dummyRelay.getNumberRemoved(DB_NAME + "_1"));
    // partId = 2; added = 2, removed = 1
    Assert.assertEquals(2, _dummyRelay.getNumberAdded(DB_NAME + "_2"));
    Assert.assertEquals(1, _dummyRelay.getNumberRemoved(DB_NAME + "_2"));
    // partId = 3; added = 3, removed = 2
    Assert.assertEquals(3, _dummyRelay.getNumberAdded(DB_NAME + "_3"));
    Assert.assertEquals(2, _dummyRelay.getNumberRemoved(DB_NAME + "_3"));
  }

  @Test
  public void testDropDatabase() throws Exception {
    ClusterManagerPhysicalSourceConfigBuilder cmPSCB = 
        new ClusterManagerPhysicalSourceConfigBuilder(null, _sourceIdNameRegistry, _schemaRegistry, _dummyRelay); 
    ResourceManager rm  = new ResourceManager();
    rm.setListener(cmPSCB); // listener will be called on add/remove calls
    
    // add 1 partition for partId = 1
    rm.addResource("ela4-db1-espresso.prod.linkedin.com_1521,"+DB_NAME+",p1_1,MASTER");
    
    // Drop database
    rm.dropDatabase(DB_NAME);
    
    Assert.assertEquals(_dummyRelay._dropped, true);
  }
  
  
}
