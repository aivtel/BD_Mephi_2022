package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.*;

public class HW1Mapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private final static IntWritable quantityOne = new IntWritable(1); // variable value initialization
    private static final String[] codeNames =
            {"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"};
    private Text dateAndHour = new Text();
    private Text codeNameKey = new Text();
    private static String regexStr = "^20[0-9]{2}-(0[1-9]|1[0-2])-(0[1-9]|1[0-9]|2[0-9]|3[0-1]) (0[0-9]|1[0-9]|2[0-3])(:(0[0-9]|[1-5][0-9])){2},[0-7]$"; //регулярное выражение
    private static Integer indexOfCode;
    Pattern patternRegex = Pattern.compile(regexStr);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher matcher = patternRegex.matcher(line);
        if (!matcher.find()) {
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            String[] stringElements = line.split("[:,]"); //split string with timestamp and error code
            String codeName = "";
            MapWritable codeDict = new MapWritable();
            if (!stringElements[0].isEmpty() & !stringElements[stringElements.length - 1].isEmpty()) {
                dateAndHour.set(stringElements[0]);
                indexOfCode = Integer.parseInt(stringElements[stringElements.length - 1]);
                codeName = codeNames[indexOfCode]; // get code name: crit, alert, etc.
                codeNameKey.set(codeName);
                codeDict.put(codeNameKey, quantityOne); // MapWritable dict { alert=1 }
                context.write(dateAndHour, codeDict);
            }
        }
    }
}
