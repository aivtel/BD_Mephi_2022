import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, MapWritable> mapDriver;
    private ReduceDriver<Text, MapWritable, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, MapWritable, Text, Text> mapReduceDriver;

    private final String testLine = "2022-04-06 06:06:06,1";
    private final String firstMock = "2022-04-06 06:07:07,3";
    private final String secondMock = "2022-04-06 06:08:08,4";
    private final String timestampMock = "2022-04-06 06";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        MapWritable resultTestMapper = new MapWritable();
        resultTestMapper.put(new Text("alert"), new IntWritable(1));
        mapDriver
                .withInput(new LongWritable(), new Text(testLine))
                .withOutput(new Text("2022-04-06 06"), resultTestMapper)
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<MapWritable> values = new ArrayList<MapWritable>();
        MapWritable firstValue = new MapWritable();
        firstValue.put(new Text("alert"), new IntWritable(1));
        MapWritable secondValue = new MapWritable();
        secondValue.put(new Text("err"), new IntWritable(1));
        MapWritable thirdValue = new MapWritable();
        thirdValue.put(new Text("alert"), new IntWritable(1));
        MapWritable fourthValue = new MapWritable();
        fourthValue.put(new Text("alert"), new IntWritable(1));
        MapWritable fifthValue = new MapWritable();
        fifthValue.put(new Text("crit"), new IntWritable(1));
        values.add(firstValue);
        values.add(secondValue);
        values.add(thirdValue);
        values.add(fourthValue);
        values.add(fifthValue);
        Text result = new Text(" err: 1;  crit: 1;  alert: 3; ");
        reduceDriver
                .withInput(new Text(timestampMock), values)
                .withOutput(new Text(timestampMock), result)
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        Text result = new Text(" err: 1;  warning: 1; ");
        mapReduceDriver
                .withInput(new LongWritable(), new Text(firstMock))
                .withInput(new LongWritable(), new Text(secondMock))
                .withOutput(new Text(timestampMock), result)
                .runTest();
    }
}