package com.linkedin.databus3.espresso.cmclient;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.linkedin.databus.core.cmclient.ResourceKey;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

public class MockPhysicalSourceConfigBuilder extends BasePhysicalSourceConfigBuilder implements ConfigBuilder<List<PhysicalSourceStaticConfig>>
{
	/**
	 * A collection of resource keys with which the PhysicalSourceConfigBuilder is constructed
	 * In the actual case, this collection is obtained from the cluster manager
	 */
	private Vector<ResourceKey> _rks;


	/**
	 * @param rks Collection of resource keys
	 *
 	 * @see ResourceKey
	 */
	public MockPhysicalSourceConfigBuilder(Vector<ResourceKey> rks) {
		_rks = rks;
	}

	public MockPhysicalSourceConfigBuilder() {
	}

	public PhysicalSourceStaticConfig build(ResourceKey rk) throws InvalidConfigException {

    PhysicalSourceConfig pssc = buildOnePSSCFromResourceKey(rk);
    return pssc.build();
  }
	/**
	 * Builder
	 */
	@Override
	public List<PhysicalSourceStaticConfig> build() throws InvalidConfigException {
		Vector<ResourceKey> rks = getResourceKeys();
		if (rks.isEmpty()) {
			return new ArrayList<PhysicalSourceStaticConfig>();
		}

		List<PhysicalSourceStaticConfig> psscList = buildPSSCFromResourceKeys(rks);
		return psscList;
	}

	/**
	 * Actual builder gets the logical source ( database name ) from SourceIdRegistry
	 * For unit-testing, we mock the database name
	 */
	@Override
	protected List<String> getLogicalSourcesFromRegistry(String dbName)
	throws InvalidConfigException {
		List<String> logicalSources = new ArrayList<String>();

		if (dbName.equals("BizFollow")){
			logicalSources.add("BizFollow.BizFollowData");
		}
		if (dbName.equals("ucpx")){
      logicalSources.add("ucpx.Email");
    }
		else if (dbName.equals("BizFollowCachingTest")){
		    logicalSources = super.getLogicalSourcesFromRegistry("BizFollow");
		}
		return logicalSources;
	}

	@Override
	protected long getLogicalSourceId(String name)
	throws InvalidConfigException {
		return 101;
	}

	/**
	 *
	 * @return Collection of resource keys
	 * @throws InvalidConfigException
	 */
	private Vector<ResourceKey> getResourceKeys()
	throws InvalidConfigException {
		return _rks;
	}
}
