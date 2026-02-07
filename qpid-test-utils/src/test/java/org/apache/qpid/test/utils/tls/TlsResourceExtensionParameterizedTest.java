package org.apache.qpid.test.utils.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Execution(ExecutionMode.SAME_THREAD)
@ExtendWith({ TlsResourceExtension.class })
class TlsResourceExtensionParameterizedTest
{
    private static final List<Integer> PARAMETERIZED_IDS = new ArrayList<>();
    private static final List<Integer> REPEATED_IDS = new ArrayList<>();

    private TlsResource _beforeEachResource;

    @BeforeEach
    void setUp(final TlsResource tls)
    {
        _beforeEachResource = tls;
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 3 })
    void parameterizedTestUsesSameInstanceWithinInvocation(final int value, final TlsResource tls)
    {
        assertNotNull(value);
        assertSame(_beforeEachResource, tls);
        PARAMETERIZED_IDS.add(System.identityHashCode(tls));
    }

    @RepeatedTest(3)
    void repeatedTestUsesSameInstanceWithinInvocation(final TlsResource tls)
    {
        assertSame(_beforeEachResource, tls);
        REPEATED_IDS.add(System.identityHashCode(tls));
    }

    @AfterAll
    static void verifyInstancesAcrossInvocations()
    {
        assertEquals(3, PARAMETERIZED_IDS.size());
        assertEquals(3, REPEATED_IDS.size());
        assertEquals(3, PARAMETERIZED_IDS.stream().distinct().count());
        assertEquals(3, REPEATED_IDS.stream().distinct().count());
    }
}
