/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TestTableGauges
{
    private TableGauges myTableGauges;

    @Before
    public void init()
    {
        myTableGauges = new TableGauges();
    }

    @Test
    public void testUpdateLastRepairedAt()
    {
        long expectedLastRepaired = 1234;

        myTableGauges.lastRepairedAt(expectedLastRepaired);

        assertThat(myTableGauges.getLastRepairedAt()).isEqualTo(expectedLastRepaired);
    }

    @Test
    public void testUpdateRemainingRepairTime()
    {
        long remainingRepairTime = 1234L;

        myTableGauges.remainingRepairTime(remainingRepairTime);

        assertThat(myTableGauges.getRemainingRepairTime()).isEqualTo(remainingRepairTime);
    }

    @Test
    public void testUpdateRepairRatioAllRepaired()
    {
        double expectedRatio = 1.0;

        myTableGauges.repairedRatio(expectedRatio);

        assertThat(myTableGauges.getRepairedRatio()).isEqualTo(expectedRatio);
    }

    @Test
    public void testUpdateRepairRatioHalfRepaired()
    {
        double expectedRatio = 0.5;

        myTableGauges.repairedRatio(expectedRatio);

        assertThat(myTableGauges.getRepairedRatio()).isEqualTo(expectedRatio);
    }

    @Test
    public void testUpdateRepairRatioNothingRepaired()
    {
        double expectedRatio = 0.0;

        myTableGauges.repairedRatio(expectedRatio);

        assertThat(myTableGauges.getRepairedRatio()).isEqualTo(expectedRatio);
    }
}
