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
package com.datatorrent.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import com.datatorrent.lib.db.jdbc.AbstractJdbcTransactionableOutputOperator;

public class JdbcWriter extends AbstractJdbcTransactionableOutputOperator<JdbcWriter.Employee>
{
  public static final String upsertStmt = "MERGE INTO Employee USING dual ON ( id = ? ) "
      + " WHEN MATCHED THEN UPDATE SET lastName = ?, firstName = ? "
      + " WHEN NOT MATCHED THEN INSERT (id, lastName, firstName) VALUES ( ?, ?, ? )";

  @Nonnull
  @Override
  protected String getUpdateCommand()
  {
    return upsertStmt;
  }

  @Override
  protected void setStatementParameters(PreparedStatement statement, Employee tuple) throws SQLException
  {
    statement.setLong(1, tuple.id);
    statement.setString(2, tuple.lastName);
    statement.setString(3, tuple.firstName);
    statement.setLong(4, tuple.id);
    statement.setString(5, tuple.lastName);
    statement.setString(6, tuple.firstName);
  }

  static class Employee
  {
    long id;
    String firstName;
    String lastName;
  }
}
