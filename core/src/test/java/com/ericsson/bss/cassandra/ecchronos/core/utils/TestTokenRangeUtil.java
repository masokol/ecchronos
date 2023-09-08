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

import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTokenRangeUtil
{
    private static final BigInteger RANGE_START = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger RANGE_END = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger FULL_RANGE = RANGE_END.subtract(RANGE_START).add(BigInteger.ONE);

    @Test
    public void testGenerateNoCombineAndNoSplits()
    {
        LongTokenRange range1 = new LongTokenRange(1, 4);
        LongTokenRange range2 = new LongTokenRange(4, 7);
        LongTokenRange range3 = new LongTokenRange(7, 10);
        LongTokenRange range4 = new LongTokenRange(10, 13);
        LongTokenRange range5 = new LongTokenRange(13, 16);
        Map<Integer, Set<LongTokenRange>> combinedRanges = TokenRangeUtil.generateRanges(List.of(range1, range2, range3, range4, range5), BigInteger.ZERO);
        assertThat(combinedRanges).hasSize(5);
        assertThat(combinedRanges.get(0)).hasSize(1);
        assertThat(combinedRanges.get(0)).containsExactly(range1);
        assertThat(combinedRanges.get(1)).hasSize(1);
        assertThat(combinedRanges.get(1)).containsExactly(range2);
        assertThat(combinedRanges.get(2)).hasSize(1);
        assertThat(combinedRanges.get(2)).containsExactly(range3);
        assertThat(combinedRanges.get(3)).hasSize(1);
        assertThat(combinedRanges.get(3)).containsExactly(range4);
        assertThat(combinedRanges.get(4)).hasSize(1);
        assertThat(combinedRanges.get(4)).containsExactly(range5);
    }

    @Test
    public void testGenerateCombinedTwoCombines()
    {
        LongTokenRange range1 = new LongTokenRange(1, 4);
        LongTokenRange range2 = new LongTokenRange(4, 7);
        LongTokenRange range3 = new LongTokenRange(7, 10);
        LongTokenRange range4 = new LongTokenRange(10, 13);
        LongTokenRange range5 = new LongTokenRange(13, 16);
        Map<Integer, Set<LongTokenRange>> combinedRanges = TokenRangeUtil.generateRanges(List.of(range1, range2, range3, range4, range5), BigInteger.valueOf(9));
        assertThat(combinedRanges).hasSize(2);
        assertThat(combinedRanges.get(0)).hasSize(3);
        assertThat(combinedRanges.get(0)).containsExactly(range1, range2, range3);
        assertThat(combinedRanges.get(1)).hasSize(2);
        assertThat(combinedRanges.get(1)).containsExactly(range4, range5);
    }

    @Test
    public void testGenerateCombinedThreeCombines()
    {
        LongTokenRange range1 = new LongTokenRange(1, 4);
        LongTokenRange range2 = new LongTokenRange(4, 7);
        LongTokenRange range3 = new LongTokenRange(7, 10);
        LongTokenRange range4 = new LongTokenRange(10, 13);
        LongTokenRange range5 = new LongTokenRange(13, 16);
        Map<Integer, Set<LongTokenRange>> combinedRanges = TokenRangeUtil.generateRanges(List.of(range1, range2, range3, range4, range5), BigInteger.valueOf(6));
        assertThat(combinedRanges).hasSize(3);
        assertThat(combinedRanges.get(0)).hasSize(2);
        assertThat(combinedRanges.get(0)).containsExactly(range1, range2);
        assertThat(combinedRanges.get(1)).hasSize(2);
        assertThat(combinedRanges.get(1)).containsExactly(range3, range4);
        assertThat(combinedRanges.get(2)).hasSize(1);
        assertThat(combinedRanges.get(2)).containsExactly(range5);
    }

    @Test
    public void testGenerateCombinedUnevenRanges()
    {
        LongTokenRange range1 = new LongTokenRange(1, 4);
        LongTokenRange range2 = new LongTokenRange(4, 10);
        LongTokenRange range3 = new LongTokenRange(10, 13);
        LongTokenRange range4 = new LongTokenRange(13, 14);
        LongTokenRange range5 = new LongTokenRange(14, 22);
        Map<Integer, Set<LongTokenRange>> combinedRanges = TokenRangeUtil.generateRanges(List.of(range1, range2, range3, range4, range5), BigInteger.valueOf(6));
        assertThat(combinedRanges).hasSize(4);
        assertThat(combinedRanges.get(0)).hasSize(1);
        assertThat(combinedRanges.get(0)).containsExactly(range1);
        assertThat(combinedRanges.get(1)).hasSize(1);
        assertThat(combinedRanges.get(1)).containsExactly(range2);
        assertThat(combinedRanges.get(2)).hasSize(2);
        assertThat(combinedRanges.get(2)).containsExactly(range3, range4);
        assertThat(combinedRanges.get(3)).hasSize(1);
        assertThat(combinedRanges.get(3)).containsExactly(range5);

    }

    @Test
    public void testGenerateCombinedWithLargerRange()
    {
        LongTokenRange range1 = new LongTokenRange(1, 4);
        LongTokenRange range2 = new LongTokenRange(4, 7);
        LongTokenRange range3 = new LongTokenRange(7, 10);
        LongTokenRange range4 = new LongTokenRange(10, 13);
        LongTokenRange range5 = new LongTokenRange(13, 16);
        Map<Integer, Set<LongTokenRange>> combinedRanges = TokenRangeUtil.generateRanges(List.of(range1, range2, range3, range4, range5), BigInteger.valueOf(1000));
        assertThat(combinedRanges).hasSize(1);
        assertThat(combinedRanges.get(0)).hasSize(5);
        assertThat(combinedRanges.get(0)).containsExactly(range1, range2, range3, range4, range5);
    }

    @Test
    public void testGenerateCombinedNegative()
    {
        LongTokenRange range1 = new LongTokenRange(-10, -7);
        LongTokenRange range2 = new LongTokenRange(-7, -4);
        LongTokenRange range3 = new LongTokenRange(-4, -1);
        LongTokenRange range4 = new LongTokenRange(-1, 2);
        LongTokenRange range5 = new LongTokenRange(2, 5);
        Map<Integer, Set<LongTokenRange>> combinedRanges = TokenRangeUtil.generateRanges(List.of(range1, range2, range3, range4, range5), BigInteger.valueOf(1000));
        assertThat(combinedRanges).hasSize(1);
        assertThat(combinedRanges.get(0)).hasSize(5);
        assertThat(combinedRanges.get(0)).containsExactly(range1, range2, range3, range4, range5);
    }
    @Test
    public void testGenerateCombinedOneForEachRange()
    {
        LongTokenRange range1 = new LongTokenRange(1, 4);
        LongTokenRange range2 = new LongTokenRange(4, 7);
        LongTokenRange range3 = new LongTokenRange(7, 10);
        Map<Integer, Set<LongTokenRange>> combinedRanges = TokenRangeUtil.generateRanges(List.of(range1, range2, range3), BigInteger.valueOf(3));
        assertThat(combinedRanges).hasSize(3);
        assertThat(combinedRanges.get(0)).hasSize(1);
        assertThat(combinedRanges.get(0)).containsExactly(range1);
        assertThat(combinedRanges.get(1)).hasSize(1);
        assertThat(combinedRanges.get(1)).containsExactly(range2);
        assertThat(combinedRanges.get(2)).hasSize(1);
        assertThat(combinedRanges.get(2)).containsExactly(range3);
    }

    @Test
    public void testGenerateOneSubRange()
    {
        LongTokenRange range = new LongTokenRange(1, 4);

        Map<Integer, Set<LongTokenRange>> subRanges = TokenRangeUtil.generateRanges(Collections.singletonList(range), range.rangeSize());

        assertThat(subRanges).hasSize(1);
        assertThat(subRanges.get(0)).hasSize(1);
        assertThat(subRanges.get(0)).containsExactly(range);
    }

    @Test
    public void testGenerateWithLargerRange()
    {
        LongTokenRange range = new LongTokenRange(1, 4);

        Map<Integer, Set<LongTokenRange>> subRanges = TokenRangeUtil.generateRanges(Collections.singletonList(range), BigInteger.valueOf(1000));

        assertThat(subRanges).hasSize(1);
        assertThat(subRanges.get(0)).hasSize(1);
        assertThat(subRanges.get(0)).containsExactly(range);
    }

    @Test
    public void testGenerateOnePerTokenNegative()
    {
        LongTokenRange range = new LongTokenRange(-5, -1);

        Map<Integer, Set<LongTokenRange>> subRanges = TokenRangeUtil.generateRanges(Collections.singletonList(range), BigInteger.ONE);

        assertThat(subRanges).hasSize(4);
        assertThat(subRanges.get(0)).hasSize(1);
        assertThat(subRanges.get(1)).hasSize(1);
        assertThat(subRanges.get(2)).hasSize(1);
        assertThat(subRanges.get(3)).hasSize(1);
        assertThat(subRanges.get(0)).containsExactly(new LongTokenRange(-5, -4));
        assertThat(subRanges.get(1)).containsExactly(new LongTokenRange(-4, -3));
        assertThat(subRanges.get(2)).containsExactly(new LongTokenRange(-3, -2));
        assertThat(subRanges.get(3)).containsExactly(new LongTokenRange(-2, -1));
    }

    @Test
    public void testGenerateOnePerToken()
    {
        LongTokenRange range = new LongTokenRange(1, 5);

        Map<Integer, Set<LongTokenRange>> subRanges = TokenRangeUtil.generateRanges(Collections.singletonList(range), BigInteger.ONE);

        assertThat(subRanges).hasSize(4);
        assertThat(subRanges.get(0)).hasSize(1);
        assertThat(subRanges.get(1)).hasSize(1);
        assertThat(subRanges.get(2)).hasSize(1);
        assertThat(subRanges.get(3)).hasSize(1);
        assertThat(subRanges.get(0)).containsExactly(new LongTokenRange(1, 2));
        assertThat(subRanges.get(1)).containsExactly(new LongTokenRange(2, 3));
        assertThat(subRanges.get(2)).containsExactly(new LongTokenRange(3, 4));
        assertThat(subRanges.get(3)).containsExactly(new LongTokenRange(4, 5));
    }

    @Test
    public void testGenerateFourNegativeSubRangesUneven()
    {
        LongTokenRange range = new LongTokenRange(-134, 0);

        Map<Integer, Set<LongTokenRange>> subRanges = TokenRangeUtil.generateRanges(Collections.singletonList(range), BigInteger.valueOf(44));

        assertThat(subRanges).hasSize(4);
        assertThat(subRanges.get(0)).hasSize(1);
        assertThat(subRanges.get(1)).hasSize(1);
        assertThat(subRanges.get(2)).hasSize(1);
        assertThat(subRanges.get(3)).hasSize(1);
        assertThat(subRanges.get(0)).containsExactly(new LongTokenRange(-134, -90));
        assertThat(subRanges.get(1)).containsExactly(new LongTokenRange(-90, -46));
        assertThat(subRanges.get(2)).containsExactly(new LongTokenRange(-46, -2));
        assertThat(subRanges.get(3)).containsExactly(new LongTokenRange(-2, 0));
    }

    @Test
    public void testGenerateFourPositiveSubRangesUneven()
    {
        LongTokenRange range = new LongTokenRange(0, 134);

        Map<Integer, Set<LongTokenRange>> subRanges = TokenRangeUtil.generateRanges(Collections.singletonList(range), BigInteger.valueOf(44));

        assertThat(subRanges).hasSize(4);
        assertThat(subRanges.get(0)).hasSize(1);
        assertThat(subRanges.get(1)).hasSize(1);
        assertThat(subRanges.get(2)).hasSize(1);
        assertThat(subRanges.get(3)).hasSize(1);
        assertThat(subRanges.get(0)).containsExactly(new LongTokenRange(0, 44));
        assertThat(subRanges.get(1)).containsExactly(new LongTokenRange(44, 88));
        assertThat(subRanges.get(2)).containsExactly(new LongTokenRange(88, 132));
        assertThat(subRanges.get(3)).containsExactly(new LongTokenRange(132, 134));
    }

    @Test
    public void testGenerateTenPositiveSubRanges()
    {
        LongTokenRange range = new LongTokenRange(0, 100);

        Map<Integer, Set<LongTokenRange>> subRanges = TokenRangeUtil.generateRanges(Collections.singletonList(range), BigInteger.TEN);

        assertThat(subRanges).hasSize(10);
        assertThat(subRanges.get(0)).hasSize(1);
        assertThat(subRanges.get(1)).hasSize(1);
        assertThat(subRanges.get(2)).hasSize(1);
        assertThat(subRanges.get(3)).hasSize(1);
        assertThat(subRanges.get(4)).hasSize(1);
        assertThat(subRanges.get(5)).hasSize(1);
        assertThat(subRanges.get(6)).hasSize(1);
        assertThat(subRanges.get(7)).hasSize(1);
        assertThat(subRanges.get(8)).hasSize(1);
        assertThat(subRanges.get(9)).hasSize(1);
        assertThat(subRanges.get(0)).containsExactly(new LongTokenRange(0, 10));
        assertThat(subRanges.get(1)).containsExactly(new LongTokenRange(10, 20));
        assertThat(subRanges.get(2)).containsExactly(new LongTokenRange(20, 30));
        assertThat(subRanges.get(3)).containsExactly(new LongTokenRange(30, 40));
        assertThat(subRanges.get(4)).containsExactly(new LongTokenRange(40, 50));
        assertThat(subRanges.get(5)).containsExactly(new LongTokenRange(50, 60));
        assertThat(subRanges.get(6)).containsExactly(new LongTokenRange(60, 70));
        assertThat(subRanges.get(7)).containsExactly(new LongTokenRange(70, 80));
        assertThat(subRanges.get(8)).containsExactly(new LongTokenRange(80, 90));
        assertThat(subRanges.get(9)).containsExactly(new LongTokenRange(90, 100));
    }

    @Test
    public void testGenerateTenNegativeSubRanges()
    {
        LongTokenRange range = new LongTokenRange(-100, 0);

        Map<Integer, Set<LongTokenRange>> subRanges = TokenRangeUtil.generateRanges(Collections.singletonList(range), BigInteger.TEN);

        assertThat(subRanges).hasSize(10);
        assertThat(subRanges.get(0)).hasSize(1);
        assertThat(subRanges.get(1)).hasSize(1);
        assertThat(subRanges.get(2)).hasSize(1);
        assertThat(subRanges.get(3)).hasSize(1);
        assertThat(subRanges.get(4)).hasSize(1);
        assertThat(subRanges.get(5)).hasSize(1);
        assertThat(subRanges.get(6)).hasSize(1);
        assertThat(subRanges.get(7)).hasSize(1);
        assertThat(subRanges.get(8)).hasSize(1);
        assertThat(subRanges.get(9)).hasSize(1);
        assertThat(subRanges.get(0)).containsExactly(new LongTokenRange(-100, -90));
        assertThat(subRanges.get(1)).containsExactly(new LongTokenRange(-90, -80));
        assertThat(subRanges.get(2)).containsExactly(new LongTokenRange(-80, -70));
        assertThat(subRanges.get(3)).containsExactly(new LongTokenRange(-70, -60));
        assertThat(subRanges.get(4)).containsExactly(new LongTokenRange(-60, -50));
        assertThat(subRanges.get(5)).containsExactly(new LongTokenRange(-50, -40));
        assertThat(subRanges.get(6)).containsExactly(new LongTokenRange(-40, -30));
        assertThat(subRanges.get(7)).containsExactly(new LongTokenRange(-30, -20));
        assertThat(subRanges.get(8)).containsExactly(new LongTokenRange(-20, -10));
        assertThat(subRanges.get(9)).containsExactly(new LongTokenRange(-10, 0));
    }

    @Test
    public void testGenerateSubRanges()
    {
        generateAndAssertSubRanges(-123456789L, 123456789L, LongTokenRange.FULL_RANGE.divide(BigInteger.valueOf(4)));
        generateAndAssertSubRanges(-123456789L, 123456789L, LongTokenRange.FULL_RANGE.divide(BigInteger.valueOf(13)));
        generateAndAssertSubRanges(-123456789L, 123456789L, LongTokenRange.FULL_RANGE.divide(BigInteger.valueOf(46)));
        generateAndAssertSubRanges(-123456789L, 123456789L, LongTokenRange.FULL_RANGE.divide(BigInteger.valueOf(128)));
        generateAndAssertSubRanges(-123456789L, 123456789L, LongTokenRange.FULL_RANGE.divide(BigInteger.valueOf(257)));
    }

    @Test
    public void testGenerateSubRangesWrapAroundClean()
    {
        LongTokenRange range = new LongTokenRange(5, -5);

        Map<Integer, Set<LongTokenRange>> subRanges = TokenRangeUtil.generateRanges(Collections.singletonList(range), LongTokenRange.FULL_RANGE.divide(BigInteger.valueOf(2)));

        assertThat(subRanges).hasSize(2);
        assertThat(subRanges.get(0)).containsExactly(new LongTokenRange(5, Long.MIN_VALUE + 5));
        assertThat(subRanges.get(1)).containsExactly(new LongTokenRange(Long.MIN_VALUE + 5, -5));
    }

    @Test
    public void testGenerateSubRangesWrapAround()
    {
        generateAndAssertSubRanges(123456789L, -123456789L, LongTokenRange.FULL_RANGE.divide(BigInteger.valueOf(4)));
        generateAndAssertSubRanges(123456789L, -123456789L, LongTokenRange.FULL_RANGE.divide(BigInteger.valueOf(13)));
        generateAndAssertSubRanges(123456789L, -123456789L, LongTokenRange.FULL_RANGE.divide(BigInteger.valueOf(46)));
        generateAndAssertSubRanges(123456789L, -123456789L, LongTokenRange.FULL_RANGE.divide(BigInteger.valueOf(128)));
        generateAndAssertSubRanges(123456789L, -123456789L, LongTokenRange.FULL_RANGE.divide(BigInteger.valueOf(257)));
    }

    private void generateAndAssertSubRanges(long start, long end, BigInteger tokensPerSubrange)
    {
        LongTokenRange longTokenRange = new LongTokenRange(start, end);
        BigInteger fullRangeSize = longTokenRange.rangeSize();
        long splitCount = fullRangeSize.divide(tokensPerSubrange).longValueExact();
        if (fullRangeSize.remainder(tokensPerSubrange).compareTo(BigInteger.ZERO) > 0)
        {
            splitCount++;
        }

        List<LongTokenRange> subRanges = new ArrayList<>();
        TokenRangeUtil.generateRanges(Collections.singletonList(longTokenRange), tokensPerSubrange).values().forEach(s -> subRanges.addAll(s));

        BigInteger biStart = BigInteger.valueOf(start);
        long lastRangeEnd = start;

        for (long i = 0; i < splitCount - 1; i++)
        {
            BigInteger expectedRangeStart = calculateSplitRangeStart(biStart, tokensPerSubrange, i);
            BigInteger expectedRangeEnd = expectedRangeStart.add(tokensPerSubrange);
            LongTokenRange expectedRange = new LongTokenRange(enforceValidBounds(expectedRangeStart), enforceValidBounds(expectedRangeEnd));
            LongTokenRange actualRange = subRanges.get((int)i);

            assertThat(actualRange.start)
                    .withFailMessage("Expecting range start to be %d for range %s, %d, %s", lastRangeEnd, actualRange, i, subRanges)
                    .isEqualTo(lastRangeEnd);
            assertThat(actualRange)
                    .withFailMessage("Expecting range %s to be %s, %d", actualRange, expectedRange, i)
                    .isEqualTo(expectedRange);
            lastRangeEnd = expectedRange.end;
        }

        // Last element should have the same end as the original range
        long lastSplit = splitCount - 1;

        BigInteger expectedRangeStart = calculateSplitRangeStart(biStart, tokensPerSubrange, lastSplit);
        LongTokenRange expectedRange = new LongTokenRange(enforceValidBounds(expectedRangeStart), end);
        LongTokenRange actualRange = subRanges.get((int)lastSplit);

        assertThat(actualRange).isEqualTo(expectedRange);

        assertRangeSizeMatch(longTokenRange, subRanges);
    }

    private void assertRangeSizeMatch(LongTokenRange fullRange, List<LongTokenRange> subRanges)
    {
        BigInteger fullRangeSize = fullRange.rangeSize();
        BigInteger actualRangeSize = BigInteger.ZERO;

        for (LongTokenRange subRange : subRanges)
        {
            actualRangeSize = actualRangeSize.add(subRange.rangeSize());
        }

        assertThat(actualRangeSize).isEqualTo(fullRangeSize);
    }

    private long enforceValidBounds(BigInteger token)
    {
        if (token.compareTo(RANGE_END) > 0)
        {
            return token.subtract(FULL_RANGE).longValueExact();
        }

        return token.longValueExact();
    }

    private BigInteger calculateSplitRangeStart(BigInteger rangeStart, BigInteger rangeSize, long sectionId)
    {
        return rangeSize.multiply(BigInteger.valueOf(sectionId)).add(rangeStart);
    }
}
