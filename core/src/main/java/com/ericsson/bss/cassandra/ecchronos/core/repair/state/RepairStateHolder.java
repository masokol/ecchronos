package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RepairStateHolder
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairStateHolder.class);
    private static final long REFRESH_INTERVAL_MS = 5000L; //TODO configurable?
    private final RepairStateFactory myRepairStateFactory;
    private final Map<CacheKey, RepairState> myRepairStates = new ConcurrentHashMap<>();
    private final ScheduledExecutorService myScheduledExecutorService;

    public RepairStateHolder(final RepairStateFactory repairStateFactory)
    {
        myRepairStateFactory = Preconditions.checkNotNull(repairStateFactory);
        myScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("RepairStateHolder-%d").build());
        myScheduledExecutorService.scheduleAtFixedRate(this::updateRepairStates,
                0,
                REFRESH_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    private void updateRepairStates()
    {
        for (CacheKey key : myRepairStates.keySet())
        {
            try
            {
                long start = System.currentTimeMillis();
                update(key.myTableReference, key.myRepairConfiguration);
                LOG.info("Scheduled repairState update took {}ms", System.currentTimeMillis() - start);
            }
            catch (Exception e)
            {
                LOG.warn("Could not update repairState for {}", key.myTableReference);
            }
        }
    }

    /**
     * Update the repairState now.
     * @param tableReference
     * @param repairConfiguration
     */
    public void update(final TableReference tableReference, final RepairConfiguration repairConfiguration)
    {
        getSnapshot(tableReference, repairConfiguration);
        myRepairStates.get(new CacheKey(tableReference, repairConfiguration)).update();
    }

    /**
     * Get snapshot for the table and repair config. If repair state doesn't exist it will be created.
     */
    public RepairStateSnapshot getSnapshot(final TableReference tableReference,
            final RepairConfiguration repairConfiguration)
    {
        return myRepairStates.computeIfAbsent(new CacheKey(tableReference, repairConfiguration),
                key -> load(key.myTableReference, key.myRepairConfiguration)).getSnapshot();
    }

    private RepairState load(final TableReference tableReference, final RepairConfiguration repairConfiguration)
    {
        return myRepairStateFactory.create(tableReference, repairConfiguration);
    }

    private class CacheKey
    {
        private TableReference myTableReference;
        private RepairConfiguration myRepairConfiguration;

        CacheKey(final TableReference tableReference, final RepairConfiguration repairConfiguration)
        {
            myTableReference = tableReference;
            myRepairConfiguration = repairConfiguration;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(myTableReference, cacheKey.myTableReference) && Objects.equals(
                    myRepairConfiguration, cacheKey.myRepairConfiguration);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(myTableReference, myRepairConfiguration);
        }
    }
}
