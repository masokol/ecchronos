/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.core.metrics;

import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;

@RunWith(MockitoJUnitRunner.class)
public class TestMetricsLogger
{
    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_TABLE1 = "test_table1";
    private static final String TEST_TABLE2 = "test_table2";
    @Mock
    private TableStorageStates myTableStorageStates;
    private MeterRegistry myMeterRegistry;
    private TableRepairMetricsImpl myTableRepairMetricsImpl;
    private MetricsLogger myMetricsLogger;

    @Before
    public void init()
    {
        // Use composite registry here to simulate real world scenario where we have multiple registries
        CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();
        // Need at least one registry present in composite to record metrics
        compositeMeterRegistry.add(new SimpleMeterRegistry());
        myMeterRegistry = compositeMeterRegistry;
        myTableRepairMetricsImpl = TableRepairMetricsImpl.builder()
                .withTableStorageStates(myTableStorageStates)
                .withMeterRegistry(myMeterRegistry)
                .build();
        myMetricsLogger = spy(new MetricsLogger(myMeterRegistry, 2));
    }

    @After
    public void cleanup()
    {
        myMeterRegistry.close();
        myMetricsLogger.close();
    }

    @Test
    public void testThresholdPassed()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        myTableRepairMetricsImpl.repairSession(tableReference, 0L, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference, 0L, TimeUnit.MILLISECONDS, false);
        myMetricsLogger.logIfThresholdPassed();
        verify(myMetricsLogger).getLogMessage(eq(TEST_KEYSPACE), eq(TEST_TABLE1), eq(2L));
    }

    @Test
    public void testThresholdPassedMultipleTables()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);
        myTableRepairMetricsImpl.repairSession(tableReference, 0L, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference2, 0L, TimeUnit.MILLISECONDS, false);
        myMetricsLogger.logIfThresholdPassed();
        verify(myMetricsLogger).getLogMessage(eq(TEST_KEYSPACE), eq(TEST_TABLE1), eq(1L));
        verify(myMetricsLogger).getLogMessage(eq(TEST_KEYSPACE), eq(TEST_TABLE2), eq(1L));
    }

    @Test
    public void testNoLogBelowThreshold()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        myTableRepairMetricsImpl.repairSession(tableReference, 0L, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference, 0L, TimeUnit.MILLISECONDS, true);
        myMetricsLogger.logIfThresholdPassed();
        verify(myMetricsLogger, never()).getLogMessage(any(String.class), any(String.class), any(long.class));
    }

    @Test
    public void testBelowThresholdThenAbove()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        myTableRepairMetricsImpl.repairSession(tableReference, 0L, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference, 0L, TimeUnit.MILLISECONDS, true);
        myMetricsLogger.logIfThresholdPassed();
        verify(myMetricsLogger, never()).getLogMessage(any(String.class), any(String.class), any(long.class));
        myTableRepairMetricsImpl.repairSession(tableReference, 0L, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference, 0L, TimeUnit.MILLISECONDS, false);
        myMetricsLogger.logIfThresholdPassed();
        verify(myMetricsLogger).getLogMessage(eq(TEST_KEYSPACE), eq(TEST_TABLE1), eq(2L));
    }

    @Test
    public void testNoLogNoMeters()
    {
        myMetricsLogger.logIfThresholdPassed();
        verify(myMetricsLogger, never()).getLogMessage(any(String.class), any(String.class), any(long.class));
    }
}
