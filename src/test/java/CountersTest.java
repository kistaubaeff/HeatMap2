import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    private final String testMalformedRegion = "90344 42348 34 324";
    private final String testRegion = "650 10 802 59354";
    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperCounterOne() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(testMalformedRegion))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounterZero() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testRegion))
                .withOutput(new Text("lower_right"), new IntWritable(1))
                .runTest();
        assertEquals("Expected 0 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testRegion))
                .withInput(new LongWritable(), new Text(testMalformedRegion))
                .withInput(new LongWritable(), new Text(testMalformedRegion))
                .withOutput(new Text("lower_right"), new IntWritable(1))
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
}

