package bdtc.lab1;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

@Log4j
public class HW1Reducer extends Reducer<Text, MapWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> dict = new HashMap<String, Integer>();
        while (values.iterator().hasNext()) {
            MapWritable codesInfoDict = values.iterator().next(); // {alert=8, crit=2, ...}
            Set<Writable> keys = codesInfoDict.keySet(); // [alert, crit, warn ...]

            for (Writable codeName: keys) {
                String codeKey = codeName.toString();
                String valueCode = codesInfoDict.get(codeName).toString();
                int codeQuantity = Integer.parseInt(valueCode);

                if (dict.get(codeKey) != null) {
                    int upgradedCodeQuantity = dict.get(codeKey) + codeQuantity; // sum prev value with current value
                    dict.put(codeKey, upgradedCodeQuantity); // upgrade dict values by the key
                }
                else {
                    dict.put(codeKey, codeQuantity); // put new record to the dict
                }
            }

        }

        String resultMessageLine = "";
        Enumeration<String> codeKeys = Collections.enumeration(dict.keySet());
        while(codeKeys.hasMoreElements()) { // loop by code values in the dict, adding to result string
            String currentCodeKey = codeKeys.nextElement();
            resultMessageLine = resultMessageLine + " " + currentCodeKey + ": " + dict.get(currentCodeKey) + "; ";
        }
        Text value = new Text(resultMessageLine);
        context.write(key, value);
    }
}
