package com.linkedin.databus3.espresso.schema;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import com.linkedin.databus2.core.DatabusException;

public class EspressoSchemaName
{
  public static final char DB_DOC_SEPARATOR = '.';

  private final String _dbName;
  private final String _tableName;
  private final String _dbusSourceName;

  private EspressoSchemaName(String dbName, String tableName, String dbusSourceName)
  {
    super();
    _dbName = dbName;
    _tableName = tableName;
    _dbusSourceName = dbusSourceName;
  }

  public static EspressoSchemaName create(String dbName, String tableName)
  {
    return new EspressoSchemaName(dbName, tableName, dbName + DB_DOC_SEPARATOR + tableName);
  }

  public static EspressoSchemaName create(String dbusSourceName) throws DatabusException
  {
    int separatorIdx = dbusSourceName.indexOf(DB_DOC_SEPARATOR);
    if (0 >= separatorIdx || separatorIdx == dbusSourceName.length() - 1)
    {
      throw new DatabusException("invalid source name: " + dbusSourceName);
    }
    return new EspressoSchemaName(dbusSourceName.substring(0, separatorIdx),
                                  dbusSourceName.substring(separatorIdx + 1),
                                  dbusSourceName);
  }

  public String getDatabusSourceName()
  {
    return _dbusSourceName;
  }

  public String getDbName()
  {
    return _dbName;
  }

  public String getTableName()
  {
    return _tableName;
  }

}
