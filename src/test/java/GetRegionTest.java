import static bdtc.lab1.HW1Mapper.getRegion;
import static org.junit.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class GetRegionTest {

    private Map<String, String> areas;

    @Before
    public void setUp() {
        areas = new HashMap<>();
        areas.put("lower_right", "500 0 1000 750");
        areas.put("upper_right", "500 750 1000 1500");
        areas.put("lower_left", "0 0 500 750");
        areas.put("upper_left", "0 750 500 1500");
    }

    @Test
    public void getLowerLeftTest() {
        assertEquals("Expected lower left region", "lower_left", getRegion(5, 5, areas));
    }

    @Test
    public void getUpperLeftTest() {
        assertEquals("Expected upper left region", "upper_left", getRegion(250, 1000, areas));
    }

    @Test
    public void getUpperRightTest() {
        assertEquals("Expected upper right region", "upper_right", getRegion(750, 1000, areas));
    }

    @Test
    public void getLowerRightTest() {
        assertEquals("Expected lower right region", "lower_right", getRegion(750, 375, areas));
    }

    @Test
    public void getUnknownTest() {
        assertEquals("Expected unknown  region", "unknown", getRegion(-5, -5, areas));
    }

}
