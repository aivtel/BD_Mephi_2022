import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private MapDriver<LongWritable, Text, Text, MapWritable> mapDriver;

    private final String incorrectLine = "2022-04-203323 06:06:06,1";
    private final String correctLine = "2022-04-20 06:06:06,1";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperCounterOne() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(incorrectLine))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounterZero() throws IOException {
        MapWritable resultTestMapper = new MapWritable();
        resultTestMapper.put(new Text("alert"), new IntWritable(1));
        mapDriver
                .withInput(new LongWritable(), new Text(correctLine))
                .withOutput(new Text("2022-04-20 06"), resultTestMapper)
                .runTest();
        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounters() throws IOException {
        MapWritable resultTestMapper = new MapWritable();
        resultTestMapper.put(new Text("alert"), new IntWritable(1));
        mapDriver
                .withInput(new LongWritable(), new Text(correctLine))
                .withInput(new LongWritable(), new Text(incorrectLine))
                .withInput(new LongWritable(), new Text(incorrectLine))
                .withOutput(new Text("2022-04-20 06"), resultTestMapper)
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
}

