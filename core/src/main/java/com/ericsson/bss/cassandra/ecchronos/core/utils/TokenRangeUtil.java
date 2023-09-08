/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.InternalException;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class to split a token range into smaller sub-ranges or combine token ranges
 * based on wanted tokens per repair.
 */
public final class TokenRangeUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(TokenRangeUtil.class);

    private TokenRangeUtil()
    {
        // Utility class
    }

    /**
     * Generates ranges based on input ranges and wanted tokens per repair.
     * The ranges can be split, combined or left unchanged.
     * @param ranges The ranges to combine or split.
     * @param tokensPerRepairTask The wanted tokens per repair task.
     * @return Ordered map with ordered ranges as values. Each key-value pair represents a single repair task.
     */
    public static Map<Integer, Set<LongTokenRange>> generateRanges(final List<LongTokenRange> ranges,
            final BigInteger tokensPerRepairTask)
    {
        Map<Integer, Set<LongTokenRange>> processedRanges = new LinkedHashMap<>();
        // Just copy the ranges into map
        // I.e, same functionality as in old ecChronos version with no target repair size set.
        if (tokensPerRepairTask.compareTo(BigInteger.ZERO) == 0)
        {
            LOG.trace("Returning ranges as they are");
            int i = 0;
            for (LongTokenRange range : ranges)
            {
                processedRanges.put(i, Collections.singleton(range));
                i++;
            }
            return processedRanges;
        }
        if (needsSplitting(ranges.get(0), tokensPerRepairTask))
        {
            LOG.trace("Maybe splitting ranges");
            // Split the ranges
            int i = 0;
            for (LongTokenRange range : ranges)
            {
                List<LongTokenRange> subranges = generateSubRanges(range, tokensPerRepairTask);
                for (LongTokenRange subrange : subranges)
                {
                    processedRanges.put(i, Collections.singleton(subrange));
                    i++;
                }
            }
        }
        else
        {
            LOG.trace("Maybe combining ranges");
            // Combine the ranges
            processedRanges = getCombinedRanges(ranges, tokensPerRepairTask);
        }
        return processedRanges;
    }

    private static boolean needsSplitting(final LongTokenRange range, final BigInteger tokensPerRepairTask)
    {
        return (range.rangeSize().compareTo(tokensPerRepairTask) > 0);
    }

    private static Map<Integer, Set<LongTokenRange>> getCombinedRanges(final List<LongTokenRange> ranges,
            final BigInteger tokensPerRepairTask)
    {
        Map<Integer, Set<LongTokenRange>> combinedRanges = new LinkedHashMap<>();
        int index = 0;
        BigInteger totalCombinedSize = BigInteger.ZERO;
        for (LongTokenRange range : ranges)
        {
            // If adding the range doesn't fit into tokensPerRepairTask then move index.
            // Index represents separate repair tasks.
            if (totalCombinedSize.add(range.rangeSize()).compareTo(tokensPerRepairTask) > 0)
            {
                index++;
                totalCombinedSize = BigInteger.ZERO;
            }
            Set<LongTokenRange> cr = combinedRanges.getOrDefault(index, new LinkedHashSet<>());
            cr.add(range);
            combinedRanges.put(index, cr);
            totalCombinedSize = totalCombinedSize.add(range.rangeSize());
        }
        return combinedRanges;
    }

    private static List<LongTokenRange> generateSubRanges(final LongTokenRange tokenRange,
            final BigInteger tokenPerSubRange)
    {
        if (!needsSplitting(tokenRange, tokenPerSubRange))
        {
            return Lists.newArrayList(tokenRange); // Full range is smaller than wanted tokens
        }
        BigInteger totalRangeSize = tokenRange.rangeSize();
        long actualSubRangeCount = totalRangeSize.divide(tokenPerSubRange).longValueExact();
        if (totalRangeSize.remainder(tokenPerSubRange).compareTo(BigInteger.ZERO) > 0)
        {
            actualSubRangeCount++;
        }

        List<LongTokenRange> subRanges = new ArrayList<>();
        for (long l = 0; l < actualSubRangeCount - 1; l++)
        {
            subRanges.add(newSubRange(BigInteger.valueOf(tokenRange.start), tokenPerSubRange, l));
        }

        LongTokenRange lastRange = subRanges.get(subRanges.size() - 1);
        subRanges.add(new LongTokenRange(lastRange.end, tokenRange.end));

        // Verify sub range size match full range size
        validateSubRangeSize(totalRangeSize, tokenRange, subRanges);

        return subRanges;
    }

    private static void validateSubRangeSize(final BigInteger totalRangeSize, final LongTokenRange tokenRange,
            final List<LongTokenRange> subRanges)
    {
        BigInteger subRangeSize = BigInteger.ZERO;

        for (LongTokenRange range : subRanges)
        {
            subRangeSize = subRangeSize.add(range.rangeSize());
        }

        if (subRangeSize.compareTo(totalRangeSize) != 0)
        {
            BigInteger difference = totalRangeSize.subtract(subRangeSize).abs();
            String message = String.format(
                    "Unexpected sub-range generation for %s. Difference of %s. Sub-ranges generated: %s",
                    tokenRange, difference, subRanges);

            LOG.error(message);
            throw new InternalException(message);
        }
    }

    private static LongTokenRange newSubRange(final BigInteger tokenStart, final BigInteger rangeSize,
            final long rangeId)
    {
        BigInteger rangeOffset = rangeSize.multiply(BigInteger.valueOf(rangeId));
        BigInteger rangeStartTmp = tokenStart.add(rangeOffset);
        BigInteger rangeEndTmp = rangeStartTmp.add(rangeSize);

        long rangeStart = enforceValidBounds(rangeStartTmp);
        long rangeEnd = enforceValidBounds(rangeEndTmp);

        return new LongTokenRange(rangeStart, rangeEnd);
    }

    private static long enforceValidBounds(final BigInteger tokenValue)
    {
        if (tokenValue.compareTo(LongTokenRange.RANGE_END) > 0)
        {
            return tokenValue.subtract(LongTokenRange.FULL_RANGE).longValueExact();
        }

        return tokenValue.longValueExact();
    }
}
