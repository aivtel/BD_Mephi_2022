package bdtc.lab1;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

@Log4j
public class HW1Combiner extends Reducer<Text, MapWritable, Text, MapWritable> {
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        MapWritable upgradedDict = new MapWritable();
        while (values.iterator().hasNext()) {
            MapWritable codesInfoDict = values.iterator().next(); // { alert=1 } or {crit=1} etc.
            Set<Writable> keys = codesInfoDict.keySet(); // [alert, crit, warn, ...]

            for (Writable codeName: keys) {
                Writable valueCode = codesInfoDict.get(codeName);

                if (upgradedDict.get(codeName) != null) {
                    IntWritable newValueCode = new IntWritable();
                    int prevValue = Integer.parseInt(upgradedDict.get(codeName).toString());
                    int addValue = Integer.parseInt((valueCode.toString()));
                    newValueCode.set(prevValue + addValue);
                    upgradedDict.put(codeName, newValueCode);
                }
                else {
                    upgradedDict.put(codeName, valueCode);
                }
            }

        }

        context.write(key, upgradedDict);
    }
}
