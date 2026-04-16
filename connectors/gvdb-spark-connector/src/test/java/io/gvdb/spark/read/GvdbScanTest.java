package io.gvdb.spark.read;

import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GvdbScanTest {

    @Test
    void inferMetadataTypeLong() {
        assertEquals(DataTypes.LongType, GvdbScan.inferMetadataType(42L));
    }

    @Test
    void inferMetadataTypeInteger() {
        assertEquals(DataTypes.LongType, GvdbScan.inferMetadataType(42));
    }

    @Test
    void inferMetadataTypeDouble() {
        assertEquals(DataTypes.DoubleType, GvdbScan.inferMetadataType(3.14));
    }

    @Test
    void inferMetadataTypeFloat() {
        assertEquals(DataTypes.DoubleType, GvdbScan.inferMetadataType(3.14f));
    }

    @Test
    void inferMetadataTypeString() {
        assertEquals(DataTypes.StringType, GvdbScan.inferMetadataType("hello"));
    }

    @Test
    void inferMetadataTypeBoolean() {
        assertEquals(DataTypes.BooleanType, GvdbScan.inferMetadataType(true));
    }

    @Test
    void inferMetadataTypeUnsupportedReturnsNull() {
        assertNull(GvdbScan.inferMetadataType(new byte[]{1, 2, 3}));
    }
}
