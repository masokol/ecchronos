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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.search.Search;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetricsImpl.KEYSPACE_TAG;
import static com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetricsImpl.SUCCESSFUL_TAG;
import static com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetricsImpl.TABLE_TAG;

/**
 * The aim of this class is to log metrics that pass a certain threshold.
 * This will make debugging issues a lot easier when statistics are disabled.
 */
public class MetricsLogger implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(MetricsLogger.class);
    private static final int METRICS_LOGGER_INTERVAL_MINUTES = 10;

    private final MeterRegistry myMeterRegistry;
    private final long myFailedRepairSessionsThreshold;
    private final ScheduledExecutorService myExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("MetricsLogger-%d").build());
    private final Map<Meter.Id, Long> myLastFailedRepairSessionsCount = new HashMap<>();

    public MetricsLogger(final MeterRegistry meterRegistry, final long failedRepairSessionsThreshold)
    {
        myMeterRegistry = Preconditions.checkNotNull(meterRegistry, "Meter registry cannot be null");
        myFailedRepairSessionsThreshold = failedRepairSessionsThreshold;
        myExecutor.scheduleAtFixedRate(() -> logIfThresholdPassed(), METRICS_LOGGER_INTERVAL_MINUTES,
                METRICS_LOGGER_INTERVAL_MINUTES, TimeUnit.MINUTES);
    }

    /**
     * Logs a message once the threshold passes.
     */
    void logIfThresholdPassed()
    {
        Search search = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS).tags(SUCCESSFUL_TAG, "false");
        if (search != null)
        {
            Collection<Timer> failedRepairSessionsPerTable = search.timers();
            Map<Meter.Id, Long> currentFailedRepairSessionsCount = new HashMap<>();
            for (Timer timer : failedRepairSessionsPerTable)
            {
                Meter.Id id = timer.getId();
                Long lastFailed = myLastFailedRepairSessionsCount.getOrDefault(id, 0L);
                long failedNow = timer.takeSnapshot().count();
                long failedDiff = failedNow - lastFailed;
                if (failedDiff > 0)
                {
                    currentFailedRepairSessionsCount.put(id, failedDiff);
                    myLastFailedRepairSessionsCount.put(id, failedNow);
                }
            }
            long sum = currentFailedRepairSessionsCount.values()
                    .stream()
                    .collect(Collectors.summingLong(Long::longValue));
            if (sum >= myFailedRepairSessionsThreshold)
            {
                for (Map.Entry<Meter.Id, Long> currentFailed : currentFailedRepairSessionsCount.entrySet())
                {
                    LOG.warn(getLogMessage(currentFailed.getKey().getTag(KEYSPACE_TAG),
                            currentFailed.getKey().getTag(TABLE_TAG),
                            currentFailed.getValue())); //TODO decide log level
                }
            }
        }
    }

    /**
     * Formats log message for the provided params.
     * @param keyspace The keyspace
     * @param table The table
     * @param failedSessionsDiff The amount of failed sessions since last time
     * @return Formatted log message
     */
    String getLogMessage(final String keyspace, final String table, final long failedSessionsDiff)
    {
        return String.format("Table %s.%s had %d failed repair sessions in the last %d minutes", keyspace, table,
                failedSessionsDiff, METRICS_LOGGER_INTERVAL_MINUTES);
    }

    @Override
    public final void close()
    {
        myExecutor.shutdown();
    }
}
