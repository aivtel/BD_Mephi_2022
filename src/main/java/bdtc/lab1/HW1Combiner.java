package bdtc.lab1;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

/**
 * Функция reduce Combiner'a комбинирует разрозненные справочники с единицами
 * в один справочник по ключу.
 * Если на вход со стадии маппера по ключу 2022-01-01 00 поступило шесть записей - {alert=1}, {err=1}, {alert=1}, {alert=1}, {warn=1}, {err=1}
 * то на выходе, на стадию редюсера будет передана одна запись вида
 *    key: 2022-01-01 00
 *    value: {alert=3, err=2, warn=1}
 * где в качестве value будет также использован тип MapWritable, как и на стадии маппера
 */

@Log4j
public class HW1Combiner extends Reducer<Text, MapWritable, Text, MapWritable> {
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        MapWritable upgradedDict = new MapWritable();
        while (values.iterator().hasNext()) { // цикл по записям, пришедшим с этапа маппера
            MapWritable codesInfoDict = values.iterator().next(); //достаем запись
            Set<Writable> keys = codesInfoDict.keySet(); //вытаскиваем ключи из записи (по факту будет только одно значение)

            for (Writable codeName: keys) {
                Writable valueCode = codesInfoDict.get(codeName); // достаем запись по ключу

                if (upgradedDict.get(codeName) != null) { // если справочник существует
                    IntWritable newValueCode = new IntWritable();
                    int prevValue = Integer.parseInt(upgradedDict.get(codeName).toString()); // предыдущее значение
                    int addValue = Integer.parseInt((valueCode.toString())); // текущее значение
                    newValueCode.set(prevValue + addValue); // суммируем значения
                    upgradedDict.put(codeName, newValueCode); // обновляем справочник
                }
                else {
                    upgradedDict.put(codeName, valueCode); // если записи не было, то записываем первую запись по ключу 
                }
            }

        }

        context.write(key, upgradedDict);
    }
}
