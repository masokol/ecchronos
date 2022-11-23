/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestMetricsSupplier
{
    private static final TableReference TABLE_REFERENCE = tableReference("test_keyspace", "test_table1");
    private static final TableReference TABLE_REFERENCE2 = tableReference("test_keyspace", "test_table2");

    private static final double DEFAULT_REPAIRED_RATIO = 0.5;
    private static final long DEFAULT_LAST_REPAIRED_AT = 1000L;
    private static final long DEFAULT_REMAINING_REPAIR_TIME = 500L;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private RepairState myRepairState;

    private RepairMetricSupplier myRepairMetricSupplier;

    @Before
    public void before()
    {
        initMocks(this);
        myRepairMetricSupplier = new RepairMetricSupplier(myTableRepairMetrics, 100, TimeUnit.MILLISECONDS);
        myRepairState = mockRepairState(DEFAULT_REPAIRED_RATIO, DEFAULT_LAST_REPAIRED_AT,
                DEFAULT_REMAINING_REPAIR_TIME);
    }

    @After
    public void after()
    {
        myRepairMetricSupplier.close();
        reset(myTableRepairMetrics);
    }

    @Test
    public void testRegister()
    {
        myRepairMetricSupplier.register(TABLE_REFERENCE, myRepairState);

        assertThat(myRepairMetricSupplier.getRepairStates().size()).isEqualTo(1);
        assertThat(myRepairMetricSupplier.getRepairStates()).containsKey(TABLE_REFERENCE);
        assertThat(myRepairMetricSupplier.getRepairStates().get(TABLE_REFERENCE)).isEqualTo(myRepairState);
    }

    @Test
    public void testRegisterSameTableWithSameStateTwice()
    {
        myRepairMetricSupplier.register(TABLE_REFERENCE, myRepairState);
        myRepairMetricSupplier.register(TABLE_REFERENCE, myRepairState);

        assertThat(myRepairMetricSupplier.getRepairStates().size()).isEqualTo(1);
        assertThat(myRepairMetricSupplier.getRepairStates()).containsKey(TABLE_REFERENCE);
        RepairState registeredState = myRepairMetricSupplier.getRepairStates().get(TABLE_REFERENCE);
        assertThat(registeredState).isEqualTo(myRepairState);
        assertThat(registeredState.getRepairedRatio()).isEqualTo(DEFAULT_REPAIRED_RATIO);
        assertThat(registeredState.getLastRepairedAt()).isEqualTo(DEFAULT_LAST_REPAIRED_AT);
        assertThat(registeredState.getRemainingRepairTime()).isEqualTo(DEFAULT_REMAINING_REPAIR_TIME);
    }

    @Test
    public void testRegisterSameTableWithDifferentState()
    {
        myRepairMetricSupplier.register(TABLE_REFERENCE, myRepairState);

        assertThat(myRepairMetricSupplier.getRepairStates().size()).isEqualTo(1);
        assertThat(myRepairMetricSupplier.getRepairStates()).containsKey(TABLE_REFERENCE);
        RepairState registeredState = myRepairMetricSupplier.getRepairStates().get(TABLE_REFERENCE);
        assertThat(registeredState).isEqualTo(myRepairState);
        assertThat(registeredState.getRepairedRatio()).isEqualTo(DEFAULT_REPAIRED_RATIO);
        assertThat(registeredState.getLastRepairedAt()).isEqualTo(DEFAULT_LAST_REPAIRED_AT);
        assertThat(registeredState.getRemainingRepairTime()).isEqualTo(DEFAULT_REMAINING_REPAIR_TIME);

        double expectedRepairedRatio = 1.0;
        long expectedLastRepairedAt = 2345L;
        long expectedRemainingRepairTime = 0L;
        RepairState updatedRepairState = mockRepairState(expectedRepairedRatio, expectedLastRepairedAt,
                expectedRemainingRepairTime);
        myRepairMetricSupplier.register(TABLE_REFERENCE, updatedRepairState);

        assertThat(myRepairMetricSupplier.getRepairStates().size()).isEqualTo(1);
        assertThat(myRepairMetricSupplier.getRepairStates()).containsKey(TABLE_REFERENCE);
        registeredState = myRepairMetricSupplier.getRepairStates().get(TABLE_REFERENCE);
        assertThat(registeredState).isEqualTo(updatedRepairState);
        assertThat(registeredState.getRepairedRatio()).isEqualTo(expectedRepairedRatio);
        assertThat(registeredState.getLastRepairedAt()).isEqualTo(expectedLastRepairedAt);
        assertThat(registeredState.getRemainingRepairTime()).isEqualTo(expectedRemainingRepairTime);
    }

    @Test
    public void testUnregister()
    {
        myRepairMetricSupplier.register(TABLE_REFERENCE, myRepairState);

        assertThat(myRepairMetricSupplier.getRepairStates().size()).isEqualTo(1);
        assertThat(myRepairMetricSupplier.getRepairStates()).containsKey(TABLE_REFERENCE);
        assertThat(myRepairMetricSupplier.getRepairStates().get(TABLE_REFERENCE)).isEqualTo(myRepairState);
        RepairState registeredState = myRepairMetricSupplier.getRepairStates().get(TABLE_REFERENCE);
        assertThat(registeredState).isEqualTo(myRepairState);
        assertThat(registeredState.getRepairedRatio()).isEqualTo(DEFAULT_REPAIRED_RATIO);
        assertThat(registeredState.getLastRepairedAt()).isEqualTo(DEFAULT_LAST_REPAIRED_AT);
        assertThat(registeredState.getRemainingRepairTime()).isEqualTo(DEFAULT_REMAINING_REPAIR_TIME);

        myRepairMetricSupplier.unregister(TABLE_REFERENCE);
        assertThat(myRepairMetricSupplier.getRepairStates().size()).isEqualTo(0);
    }

    @Test
    public void testUnregisterNonExistingTable()
    {
        myRepairMetricSupplier.register(TABLE_REFERENCE, myRepairState);
        myRepairMetricSupplier.unregister(TABLE_REFERENCE2);

        assertThat(myRepairMetricSupplier.getRepairStates().size()).isEqualTo(1);
        assertThat(myRepairMetricSupplier.getRepairStates()).containsKey(TABLE_REFERENCE);
        RepairState registeredState = myRepairMetricSupplier.getRepairStates().get(TABLE_REFERENCE);
        assertThat(registeredState).isEqualTo(myRepairState);
        assertThat(registeredState.getRepairedRatio()).isEqualTo(DEFAULT_REPAIRED_RATIO);
        assertThat(registeredState.getLastRepairedAt()).isEqualTo(DEFAULT_LAST_REPAIRED_AT);
        assertThat(registeredState.getRemainingRepairTime()).isEqualTo(DEFAULT_REMAINING_REPAIR_TIME);
    }

    @Test
    public void testMetricsAreUpdated()
    {
        myRepairMetricSupplier.register(TABLE_REFERENCE, myRepairState);

        assertThat(myRepairMetricSupplier.getRepairStates().size()).isEqualTo(1);
        assertThat(myRepairMetricSupplier.getRepairStates()).containsKey(TABLE_REFERENCE);
        RepairState registeredState = myRepairMetricSupplier.getRepairStates().get(TABLE_REFERENCE);
        assertThat(registeredState).isEqualTo(myRepairState);
        assertThat(registeredState.getRepairedRatio()).isEqualTo(DEFAULT_REPAIRED_RATIO);
        assertThat(registeredState.getLastRepairedAt()).isEqualTo(DEFAULT_LAST_REPAIRED_AT);
        assertThat(registeredState.getRemainingRepairTime()).isEqualTo(DEFAULT_REMAINING_REPAIR_TIME);

        verify(myRepairState, timeout(5000)).updateNow();
        verify(myTableRepairMetrics, timeout(5000)).repairedRatio(TABLE_REFERENCE, DEFAULT_REPAIRED_RATIO);
        verify(myTableRepairMetrics, timeout(5000)).lastRepairedAt(TABLE_REFERENCE,
                DEFAULT_LAST_REPAIRED_AT);
        verify(myTableRepairMetrics, timeout(5000)).remainingRepairTime(TABLE_REFERENCE,
                DEFAULT_REMAINING_REPAIR_TIME);
    }

    private RepairState mockRepairState(double repairedRatio, long lastRepairedAt, long remainingRepairTime)
    {
        RepairState repairState = mock(RepairState.class);
        when(repairState.getRepairedRatio()).thenReturn(repairedRatio);
        when(repairState.getLastRepairedAt()).thenReturn(lastRepairedAt);
        when(repairState.getRemainingRepairTime()).thenReturn(remainingRepairTime);
        return repairState;
    }
}
