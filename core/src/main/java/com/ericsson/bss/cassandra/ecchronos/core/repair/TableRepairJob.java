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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateHolder;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * A scheduled job that keeps track of the repair status of a single table. The table is considered repaired for this
 * node if all the ranges this node is responsible for is repaired within the minimum run interval.
 * <p>
 * When run this job will create {@link RepairTask RepairTasks} that repairs the table.
 */
public class TableRepairJob extends ScheduledRepairJob
{
    private static final Logger LOG = LoggerFactory.getLogger(TableRepairJob.class);
    private static final int DAYS_IN_A_WEEK = 7;
    private final TableStorageStates myTableStorageStates;
    private final RepairHistory myRepairHistory;
    private final RepairStateHolder myRepairStateHolder;

    TableRepairJob(final Builder builder)
    {
        super(builder.myConfiguration, builder.myTableReference.getId(), builder.myTableReference,
                builder.myJmxProxyFactory, builder.myRepairConfiguration, builder.myRepairLockType,
                builder.myRepairPolicies, builder.myTableRepairMetrics);
        myRepairStateHolder = Preconditions.checkNotNull(builder.myRepairStateHolder,
                "Repair state holder cannot be null");
        myTableStorageStates = Preconditions
                .checkNotNull(builder.myTableStorageStates,
                        "Table storage states must be set");
        myRepairHistory = Preconditions.checkNotNull(builder.myRepairHistory,
                "Repair history must be set");
    }

    /**
     * Get scheduled repair job view.
     *
     * @return ScheduledRepairJobView
     */
    @Override
    public ScheduledRepairJobView getView()
    {
        long now = System.currentTimeMillis();
        return new ScheduledRepairJobView(getId(), getTableReference(), getRepairConfiguration(), getSnapshot(),
                getStatus(now), getProgress(now), getNextRunInMs(), RepairOptions.RepairType.VNODE);
    }

    private long getNextRunInMs()
    {
        return (getLastSuccessfulRun() + getRepairConfiguration().getRepairIntervalInMs()) - getRunOffset();
    }

    private double getProgress(final long timestamp)
    {
        long interval = getRepairConfiguration().getRepairIntervalInMs();
        Collection<VnodeRepairState> states = getSnapshot().getVnodeRepairStates().getVnodeRepairStates();

        long nRepaired = states.stream()
                .filter(isRepaired(timestamp, interval))
                .count();

        return states.isEmpty()
                ? 0
                : (double) nRepaired / states.size();
    }

    private Predicate<VnodeRepairState> isRepaired(final long timestamp, final long interval)
    {
        return state -> timestamp - state.lastRepairedAt() <= interval;
    }

    private ScheduledRepairJobView.Status getStatus(final long timestamp)
    {
        if (getRealPriority() != -1 && !super.runnable())
        {
            return ScheduledRepairJobView.Status.BLOCKED;
        }
        long repairedAt = getSnapshot().lastCompletedAt();
        long msSinceLastRepair = timestamp - repairedAt;
        RepairConfiguration config = getRepairConfiguration();

        if (msSinceLastRepair >= config.getRepairErrorTimeInMs())
        {
            return ScheduledRepairJobView.Status.OVERDUE;
        }
        if (msSinceLastRepair >= config.getRepairWarningTimeInMs())
        {
            return ScheduledRepairJobView.Status.LATE;
        }
        if (msSinceLastRepair >= (config.getRepairIntervalInMs() - getRunOffset()))
        {
            return ScheduledRepairJobView.Status.ON_TIME;
        }
        return ScheduledRepairJobView.Status.COMPLETED;
    }

    /**
     * Iterator for scheduled tasks built up by repair groups.
     *
     * @return Scheduled task iterator
     */
    @Override
    public Iterator<ScheduledTask> iterator()
    {
            List<ScheduledTask> taskList = new ArrayList<>();
            BigInteger tokensPerRepair = getTokensPerRepair(getSnapshot().getVnodeRepairStates());
            for (ReplicaRepairGroup replicaRepairGroup : getSnapshot().getRepairGroups())
            {
                RepairGroup.Builder builder = RepairGroup.newBuilder()
                        .withTableReference(getTableReference())
                        .withRepairConfiguration(getRepairConfiguration())
                        .withReplicaRepairGroup(replicaRepairGroup)
                        .withJmxProxyFactory(getJmxProxyFactory())
                        .withTableRepairMetrics(getTableRepairMetrics())
                        .withRepairResourceFactory(getRepairLockType().getLockFactory())
                        .withRepairLockFactory(REPAIR_LOCK_FACTORY)
                        .withTokensPerRepair(tokensPerRepair)
                        .withRepairPolicies(getRepairPolicies())
                        .withRepairHistory(myRepairHistory)
                        .withJobId(getId());

                taskList.add(builder.build(getRealPriority(replicaRepairGroup.getLastCompletedAt())));
            }
            return taskList.iterator();
    }

    private BigInteger getTokensPerRepair(final VnodeRepairStates vnodeRepairStates)
    {
        BigInteger tokensPerRepair = LongTokenRange.FULL_RANGE;

        if (getRepairConfiguration().getTargetRepairSizeInBytes() != RepairConfiguration.FULL_REPAIR_SIZE)
        {
            BigInteger tableSizeInBytes = BigInteger.valueOf(myTableStorageStates.getDataSize(getTableReference()));

            if (!BigInteger.ZERO.equals(tableSizeInBytes))
            {
                BigInteger fullRangeSize = vnodeRepairStates.getVnodeRepairStates().stream()
                        .map(VnodeRepairState::getTokenRange)
                        .map(LongTokenRange::rangeSize)
                        .reduce(BigInteger.ZERO, BigInteger::add);

                BigInteger targetSizeInBytes = BigInteger.valueOf(
                        getRepairConfiguration().getTargetRepairSizeInBytes());

                BigInteger targetRepairs = tableSizeInBytes.divide(targetSizeInBytes);
                tokensPerRepair = fullRangeSize.divide(targetRepairs);
            }
        }
        return tokensPerRepair;
    }

    /**
     * Update the state and set if the task was successful.
     *
     * @param successful If the task ran successfully.
     * @param task The task that has run.
     */
    @Override
    public void postExecute(final boolean successful, final ScheduledTask task)
    {
        long start = System.currentTimeMillis();
        try
        {
            myRepairStateHolder.update(getTableReference(), getRepairConfiguration());
        }
        catch (Exception e)
        {
            LOG.warn("Failed to update repairState", e);
        }
        LOG.info("Manual repairState update took {}ms", System.currentTimeMillis() - start);
        super.postExecute(successful, task);
    }

    /**
     * Get last successful run.
     *
     * @return long
     */
    @Override
    public long getLastSuccessfulRun()
    {
        return getSnapshot().lastCompletedAt();
    }

    /**
     * Get run offset.
     *
     * @return long
     */
    @Override
    public long getRunOffset()
    {
        return getSnapshot().getEstimatedRepairTime();
    }

    /**
     * Runnable.
     *
     * @return boolean
     */
    @Override
    public boolean runnable()
    {
        return getSnapshot().canRepair() && super.runnable();
    }

    /**
     * Refresh the repair state.
     */
    @Override
    public void refreshState()
    {
        myRepairStateHolder.update(getTableReference(), getRepairConfiguration());
        //TODO create tasks here?
    }

     /**
     * Calculate real priority based on available tasks.
     * @return priority
     */
    @Override
    public final int getRealPriority()
    {
        int priority = -1;
        if (getSnapshot().canRepair())
        {
            long minRepairedAt = System.currentTimeMillis();
            for (ReplicaRepairGroup replicaRepairGroup : getSnapshot().getRepairGroups())
            {
                long replicaGroupCompletedAt = replicaRepairGroup.getLastCompletedAt();
                if (replicaGroupCompletedAt < minRepairedAt)
                {
                    minRepairedAt = replicaGroupCompletedAt;
                }
            }
            priority = getRealPriority(minRepairedAt);
        }
        return priority;
    }

    private RepairStateSnapshot getSnapshot()
    {
        return myRepairStateHolder.getSnapshot(getTableReference(), getRepairConfiguration());
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("Repair job of %s", getTableReference());
    }

    @Override
    public final boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }
        TableRepairJob that = (TableRepairJob) o;
        return Objects.equals(myRepairStateHolder, that.myRepairStateHolder) && Objects.equals(myTableStorageStates,
                that.myTableStorageStates) && Objects.equals(myRepairHistory, that.myRepairHistory);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(super.hashCode(), myRepairStateHolder, myTableStorageStates, myRepairHistory);
    }

    @SuppressWarnings("VisibilityModifier")
    public static class Builder
    {
        Configuration myConfiguration = new ConfigurationBuilder()
                .withPriority(Priority.LOW)
                .withRunInterval(DAYS_IN_A_WEEK, TimeUnit.DAYS)
                .build();
        private TableReference myTableReference;
        private JmxProxyFactory myJmxProxyFactory;
        private RepairStateHolder myRepairStateHolder;
        private TableRepairMetrics myTableRepairMetrics = null;
        private RepairConfiguration myRepairConfiguration = RepairConfiguration.DEFAULT;
        private RepairLockType myRepairLockType;
        private TableStorageStates myTableStorageStates;
        private final List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();
        private RepairHistory myRepairHistory;

        /**
         * Build table repair job with configuration.
         *
         * @param configuration
         *         Configuration.
         * @return Builder
         */
        public Builder withConfiguration(final Configuration configuration)
        {
            this.myConfiguration = configuration;
            return this;
        }

        /**
         * Build table repair job with table reference.
         *
         * @param tableReference
         *         Table reference.
         * @return Builder
         */
        public Builder withTableReference(final TableReference tableReference)
        {
            myTableReference = tableReference;
            return this;
        }

        /**
         * Build table repair job with JMX proxy factory.
         *
         * @param jmxProxyFactory
         *         JMX proxy factory.
         * @return Builder
         */
        public Builder withJmxProxyFactory(final JmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        /**
         * Build table repair job with table repair metrics.
         *
         * @param tableRepairMetrics
         *         Table repair metrics.
         * @return Builder
         */
        public Builder withTableRepairMetrics(final TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        /**
         * Build table repair job with repair configuration.
         *
         * @param repairConfiguration
         *         The repair configuration.
         * @return Builder
         */
        public Builder withRepairConfiguration(final RepairConfiguration repairConfiguration)
        {
            myRepairConfiguration = repairConfiguration;
            return this;
        }

        /**
         * Build table repair job with repair lock type.
         *
         * @param repairLockType
         *         Repair lock type.
         * @return Builder
         */
        public Builder withRepairLockType(final RepairLockType repairLockType)
        {
            myRepairLockType = repairLockType;
            return this;
        }

        /**
         * Build table repair job with table storage states.
         *
         * @param tableStorageStates
         *         Table storage states.
         * @return Builder
         */
        public Builder withTableStorageStates(final TableStorageStates tableStorageStates)
        {
            myTableStorageStates = tableStorageStates;
            return this;
        }

        /**
         * Build table repair job with repair policies.
         *
         * @param repairPolicies
         *         The table repair policies.
         * @return Builder
         */
        public Builder withRepairPolices(final Collection<TableRepairPolicy> repairPolicies)
        {
            myRepairPolicies.addAll(repairPolicies);
            return this;
        }

        /**
         * Build table repair job with repair history.
         *
         * @param repairHistory
         *         Repair history.
         * @return Builder
         */
        public Builder withRepairHistory(final RepairHistory repairHistory)
        {
            myRepairHistory = repairHistory;
            return this;
        }

        /**
         * Build with repair state holder.
         * @param repairStateHolder
         * @return
         */
        public Builder withRepairStateHolder(final RepairStateHolder repairStateHolder)
        {
            myRepairStateHolder = repairStateHolder;
            return this;
        }

        /**
         * Build table repair job.
         *
         * @return TableRepairJob
         */
        public TableRepairJob build()
        {
            Preconditions.checkNotNull(myTableReference, "Table reference must be set");

            return new TableRepairJob(this);
        }
    }
}
