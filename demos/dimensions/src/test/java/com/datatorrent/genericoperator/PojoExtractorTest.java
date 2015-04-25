/*
 *  Copyright (c) 2012-2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.genericoperator;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.Lists;

/**
 *
 */
public class PojoExtractorTest
{
  public static class TestPojo
  {
    public long longField = 1;

    public int getIntField()
    {
      return 2;
    }
  }

  @Test
  public void test()
  {
    PojoExtractor pe = new PojoExtractor();
    pe.setExpressions(Lists.newArrayList("getIntField()", "longField"));
    pe.setTupleClazz(TestPojo.class);

    pe.setup();
    Object[] values = pe.execute(new TestPojo());
    Assert.assertEquals(2, values[0]);
    Assert.assertEquals((long)1, values[1]);

  }
}
