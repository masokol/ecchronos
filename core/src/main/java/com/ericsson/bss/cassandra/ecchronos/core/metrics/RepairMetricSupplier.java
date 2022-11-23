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
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/*
 * Class used to automatically update table metrics.
 */
public class RepairMetricSupplier implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairMetricSupplier.class);

    private static final int DEFAULT_UPDATE_INTERVAL_SECONDS = 5;
    private final Map<TableReference, RepairState> myRepairStates = new ConcurrentHashMap<>();
    private final ScheduledExecutorService myExecutor = Executors.newSingleThreadScheduledExecutor();
    private final TableRepairMetrics myTableRepairMetrics;

    public RepairMetricSupplier(final TableRepairMetrics tableRepairMetrics)
    {
        this(tableRepairMetrics, DEFAULT_UPDATE_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public RepairMetricSupplier(final TableRepairMetrics tableRepairMetrics, final long updateInterval,
            final TimeUnit updateIntervalUnit)
    {
        myTableRepairMetrics = tableRepairMetrics;
        myExecutor.scheduleAtFixedRate(this::updateMetrics, 0, updateInterval, updateIntervalUnit);
    }

    @VisibleForTesting
    final Map<TableReference, RepairState> getRepairStates()
    {
        return myRepairStates;
    }

    /**
     * Register a table to report metrics for.
     * @param tableReference The table to report metrics for.
     * @param repairState The repair state of the table.
     */
    public void register(final TableReference tableReference, final RepairState repairState)
    {
        LOG.info("Registered table {} for metrics", tableReference);
        myRepairStates.put(tableReference, repairState);
    }

    /**
     * Unregister a table to report metrics for.
     * @param tableReference The table to unregister.
     */
    public void unregister(final TableReference tableReference)
    {
        LOG.info("Unregistered table {} for metrics", tableReference);
        myRepairStates.remove(tableReference);
    }

    private void updateMetrics()
    {
        for (Map.Entry<TableReference, RepairState> entry : myRepairStates.entrySet())
        {
            LOG.info("Updating metrics for table {}", entry.getKey());
            entry.getValue().updateNow();
            myTableRepairMetrics.lastRepairedAt(entry.getKey(), entry.getValue().getLastRepairedAt());
            myTableRepairMetrics.repairedRatio(entry.getKey(), entry.getValue().getRepairedRatio());
            myTableRepairMetrics.remainingRepairTime(entry.getKey(), entry.getValue().getRemainingRepairTime());
        }
    }

    /**
     * Close the scheduled executor to stop reporting metrics.
     */
    @Override
    public void close()
    {
        myExecutor.shutdown();
        try
        {
            myExecutor.awaitTermination(1, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}
