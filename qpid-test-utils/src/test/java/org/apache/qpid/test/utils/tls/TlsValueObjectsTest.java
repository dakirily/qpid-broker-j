package org.apache.qpid.test.utils.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class TlsValueObjectsTest
{
    @Test
    void validityPeriodRejectsEndBeforeStart()
    {
        final Instant from = Instant.now();
        final Instant to = from.minusSeconds(1);
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new ValidityPeriod(from, to));
        assertEquals("'to' must not be before 'from'", ex.getMessage());
    }

    @Test
    void alternativeNameRejectsNullType()
    {
        final NullPointerException ex = assertThrows(NullPointerException.class,
                () -> new AlternativeName(null, "value"));
        assertEquals("type must not be null", ex.getMessage());
    }
}
