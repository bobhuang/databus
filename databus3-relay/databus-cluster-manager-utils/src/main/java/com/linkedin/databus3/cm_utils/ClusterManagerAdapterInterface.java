package com.linkedin.databus3.cm_utils;

import java.util.Map;

import com.linkedin.helix.ZNRecord;

public interface ClusterManagerAdapterInterface
{
    public ZNRecord getExternalView(boolean cached, String group); // group is dbName??
    public Map<String, ZNRecord> getExternalViews(boolean cached);
}
