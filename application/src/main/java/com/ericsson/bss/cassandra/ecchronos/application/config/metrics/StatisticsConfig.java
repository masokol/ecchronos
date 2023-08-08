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
package com.ericsson.bss.cassandra.ecchronos.application.config.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.File;

public class StatisticsConfig
{
    private static final long DEFAULT_LOG_THRESHOLD = 100L;
    private boolean myIsEnabled = true;
    private long myLogThreshold = DEFAULT_LOG_THRESHOLD;
    private File myOutputDirectory = new File("./statistics");
    private ReportingConfigs myReportingConfigs = new ReportingConfigs();
    private String myMetricsPrefix = "";

    @JsonProperty("enabled")
    public final boolean isEnabled()
    {
        return myIsEnabled;
    }

    @JsonProperty("log_threshold")
    public final long getLogThreshold()
    {
        return myLogThreshold;
    }

    @JsonProperty("log_threshold")
    public final void setLogThreshold(final long logThreshold)
    {
        myLogThreshold = logThreshold;
    }

    @JsonProperty("directory")
    public final File getOutputDirectory()
    {
        return myOutputDirectory;
    }

    @JsonProperty("reporting")
    public final ReportingConfigs getReportingConfigs()
    {
        return myReportingConfigs;
    }

    @JsonProperty("prefix")
    public final String getMetricsPrefix()
    {
        return myMetricsPrefix;
    }

    @JsonProperty("enabled")
    public final void setEnabled(final boolean enabled)
    {
        myIsEnabled = enabled;
    }

    @JsonProperty("directory")
    public final void setOutputDirectory(final String outputDirectory)
    {
        myOutputDirectory = new File(outputDirectory);
    }

    @JsonProperty("reporting")
    public final void setReportingConfigs(final ReportingConfigs reportingConfigs)
    {
        myReportingConfigs = reportingConfigs;
    }

    @JsonProperty("prefix")
    public final void setMetricsPrefix(final String metricsPrefix)
    {
        myMetricsPrefix = metricsPrefix;
    }
}
