/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.db.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;

public class CacheExampleOperator implements Operator
{

  @NotNull
  private JdbcStore store;

  private transient LoadingCache<Integer, String> map1;
  private transient LoadingCache<Integer, String> map2;

  @Override
  public void setup(Context.OperatorContext context)
  {
    store.connect();


    //building map1

    //Some form of query/object using JPA library. Using prepared statement just for example. Do similar for map 2
    final PreparedStatement query1;
    try {
      query1 = store.getConnection().prepareCall("");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    //refresh after write can be use to refresh contents synchronously
    map1 = CacheBuilder.newBuilder().refreshAfterWrite(60 * 1000, TimeUnit.MILLISECONDS).build(
        new CacheLoader<Integer, String>()
        {
          @Override
          public String load(Integer key) throws Exception
          {
            //fetch using query 1
            query1.setInt(1, key);
            ResultSet rs = query1.executeQuery();
            if (rs.next()) {
              return rs.getString(1);
            }
            return null;
          }
    });


    //To load data initially create a bulk load query/object using JPA library and populate respective maps.
    try {
      final PreparedStatement bulkQueryForMap1 = store.getConnection().prepareCall("");
      ResultSet rs = bulkQueryForMap1.executeQuery();
      while (rs.next()) {
        map1.put(rs.getInt(1), rs.getString(2));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void teardown()
  {
    store.disconnect();
  }

  public JdbcStore getStore()
  {
    return store;
  }

  public void setStore(JdbcStore store)
  {
    this.store = store;
  }
}
