/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.schemaApp;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.lib.db.jdbc.JdbcPOJOInputOperator;
import com.datatorrent.lib.db.jdbc.JdbcPOJOOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.datatorrent.lib.dedup.DeduperTimeBasedPOJOImpl;
import com.datatorrent.lib.util.FieldInfo;

@ApplicationAnnotation(name = "SchemaDemo")
public class DemoApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JdbcPOJOInputOperator jdbcInput = dag.addOperator("JdbcInput", new JdbcPOJOInputOperator());
    DeduperTimeBasedPOJOImpl deduper = dag.addOperator("Deduper", new DeduperTimeBasedPOJOImpl());
    JdbcPOJOOutputOperator jdbcOutput = dag.addOperator("JdbcOutput", new JdbcPOJOOutputOperator());

    JdbcTransactionalStore jdbcStore = new JdbcTransactionalStore();
    jdbcStore.setConnectionProperties("user:csingh,password:isKing");
    jdbcStore.setDatabaseDriver("com.mysql.jdbc.Driver");
    jdbcStore.setDatabaseUrl("jdbc:mysql://node17.morado.com:5505/csingh");

    //Configure input operator
    jdbcInput.setStore(jdbcStore);
    jdbcInput.setTableName("t1");

    FieldInfo f1 = new FieldInfo("id", "id", null);
    FieldInfo f2 = new FieldInfo("name", "name", null);
    FieldInfo f3 = new FieldInfo("ts", "time", null);
    List<FieldInfo> fieldInfos = Lists.newArrayList(f1, f2, f3);
    jdbcInput.setFieldInfos(fieldInfos);

    //Configure output operator
    jdbcOutput.setStore(jdbcStore);
    jdbcOutput.setTablename("t2");

    FieldInfo ft1 = new FieldInfo("id", "id", null);
    FieldInfo ft2 = new FieldInfo("name", "name", null);
    List<FieldInfo> t2FieldInfos = Lists.newArrayList(ft1, ft2);
    jdbcOutput.setFieldInfos(t2FieldInfos);

    //Configure deduper
    deduper.getBucketManager().setKeyExpression("name");
    deduper.getBucketManager().setTimeExpression("time");

    dag.addStream("data", jdbcInput.outputPort, deduper.inputPojo);
    dag.addStream("deduped", deduper.output, jdbcOutput.input);
  }
}
