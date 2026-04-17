package io.gvdb.client.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("GvdbVector")
class GvdbVectorTest {

    @Nested
    @DisplayName("equals / hashCode")
    class Equality {

        @Test
        @DisplayName("vectors with identical contents are equal")
        void identicalContentsEqual() {
            var a = new GvdbVector(1L, new float[]{1f, 2f, 3f}, Map.of("k", "v"));
            var b = new GvdbVector(1L, new float[]{1f, 2f, 3f}, Map.of("k", "v"));

            assertEquals(a, b);
            assertEquals(a.hashCode(), b.hashCode());
        }

        @Test
        @DisplayName("differing values make vectors unequal")
        void differentValuesNotEqual() {
            var a = new GvdbVector(1L, new float[]{1f, 2f});
            var b = new GvdbVector(1L, new float[]{1f, 3f});

            assertNotEquals(a, b);
        }

        @Test
        @DisplayName("differing ids make vectors unequal")
        void differentIdsNotEqual() {
            var a = new GvdbVector(1L, new float[]{1f});
            var b = new GvdbVector(2L, new float[]{1f});

            assertNotEquals(a, b);
        }

        @Test
        @DisplayName("HashSet deduplicates value-equal vectors")
        void hashSetDeduplicates() {
            var set = new HashSet<GvdbVector>();
            set.add(new GvdbVector(1L, new float[]{1f, 2f}));
            set.add(new GvdbVector(1L, new float[]{1f, 2f}));

            assertEquals(1, set.size());
        }
    }

    @Test
    @DisplayName("toString includes id, values, and metadata")
    void toStringContainsFields() {
        var v = new GvdbVector(42L, new float[]{0.5f}, Map.of("label", "x"));
        var s = v.toString();

        assertTrue(s.contains("42"));
        assertTrue(s.contains("0.5"));
        assertTrue(s.contains("label"));
    }

    @Test
    @DisplayName("constructor rejects null values")
    void constructorRejectsNullValues() {
        assertThrows(NullPointerException.class, () -> new GvdbVector(1L, null));
    }
}
