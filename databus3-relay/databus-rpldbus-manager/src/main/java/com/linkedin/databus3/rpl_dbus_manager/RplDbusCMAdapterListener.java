package com.linkedin.databus3.rpl_dbus_manager;

import java.util.List;
import java.util.Map;

import com.linkedin.databus3.cm_utils.ClusterManagerRelayCoordinates;
import com.linkedin.databus3.cm_utils.ClusterManagerResourceKey;

public interface RplDbusCMAdapterListener
{
  public void updateRecords(Map<ClusterManagerRelayCoordinates, List<ClusterManagerResourceKey>> _externalView);
  public void notifyUpdate();
}
