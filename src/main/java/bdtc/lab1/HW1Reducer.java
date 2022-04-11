package bdtc.lab1;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

/**
 * Функция reduce Reducer'a комбинирует разрозненные справочники (@codesInfoDict)
 * в один справочник (@dict) по ключу (Как и на этапе комбайнера). 
 * Если на вход со стадии маппера по ключу 2022-01-01 00 поступило три записи - {alert=2, warn=3, err=1}, {err=1, alert=3}, {alert=1, warn=5, err=2}
 * то будет сформирована одна суммирующая запись вида
 *    key: 2022-01-01 00
 *    value: {alert=6, err=4, warn=8}
 * Далее делаем цикл по полученному списку и добавляем в результирующую строчку название сообщения и его значение: "alert: 6, err: 4, warn: 8"
 */

@Log4j
public class HW1Reducer extends Reducer<Text, MapWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> dict = new HashMap<String, Integer>();
        while (values.iterator().hasNext()) {
            MapWritable codesInfoDict = values.iterator().next(); // достаем мини-справочник {alert=8, crit=2, ...}
            Set<Writable> keys = codesInfoDict.keySet(); // из мини-справочника достаем ключи записи (alert, crit, ...)

            for (Writable codeName: keys) { // цикл по ключам, делаем запись в итоговый справочник, суммирая предыдущие значения с текущим
                String codeKey = codeName.toString();
                String valueCode = codesInfoDict.get(codeName).toString();
                int codeQuantity = Integer.parseInt(valueCode);

                if (dict.get(codeKey) != null) { // если такая запись уже есть в итоговом справочнике
                    int upgradedCodeQuantity = dict.get(codeKey) + codeQuantity; // суммируем предыдущее значение с текущим
                    dict.put(codeKey, upgradedCodeQuantity); // обновялем запись
                }
                else {
                    dict.put(codeKey, codeQuantity); // если записи нет в итоговом справочнике, то записываем без суммирования, как есть
                }
            }

        }

        String resultMessageLine = "";
        Enumeration<String> codeKeys = Collections.enumeration(dict.keySet());
        while(codeKeys.hasMoreElements()) { // цикл по ключам итогового справочника, добавляем в строку уже существующие значения с текущими
            String currentCodeKey = codeKeys.nextElement();
            resultMessageLine = resultMessageLine + " " + currentCodeKey + ": " + dict.get(currentCodeKey) + "; ";
        }
        Text value = new Text(resultMessageLine);
        context.write(key, value);
    }
}
