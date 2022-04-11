package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.*;


/**
 * Функция map приводит входящую строку файла с данными к типу строка.
 * Проверяет с помощью регулярного выражения на соответствие формату 2022-01-01 00:00:00,1
 * Если проверка не проходит, то делаем запись по битому логу
 * Разбиваем корректную строку на элементы - первый элемент будет выступать ключом записи - MapOutputKey,
 * по нему в итоге будет выводиться информация. В нем присутствует дата и час - "2022-01-01 00" (@dateAndHour)
 * Последний элемент означает код сообщения (от нуля до семерки) @indexOfCode.
 * Это число соответствует индексу в списке наименований сообщений (@codeNames)(согласно заданию)
 * На выход передается MapOutputValue в виде MapWritable, 
 * в котором в качестве ключа выступает наименование сообщения из списка, а значением единица (т.к. на вход поступает одна запись)
 * По итогу в контекст записывается запись вида:
 *     key: 2022-01-01 00
 *     value: {alert=1} 
 */
public class HW1Mapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private final static IntWritable quantityOne = new IntWritable(1);
    private static final String[] codeNames =
            {"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"};
    private Text dateAndHour = new Text();
    private Text codeNameKey = new Text();

    //строка с регулярным выражением, проверяющая на соответствие строки входящего файла на формат 2022-01-01 00:00:00,1
    private static String regexStr = "^20[0-9]{2}-(0[1-9]|1[0-2])-(0[1-9]|1[0-9]|2[0-9]|3[0-1]) (0[0-9]|1[0-9]|2[0-3])(:(0[0-9]|[1-5][0-9])){2},[0-7]$";
    private static Integer indexOfCode;
    Pattern patternRegex = Pattern.compile(regexStr);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString(); // приводим к типу строка значение строки из входяшего файла
        Matcher matcher = patternRegex.matcher(line); // проверка на соответствие формату
        if (!matcher.find()) { // если формат не соответсвует, записываем значение в счетчик
            context.getCounter(CounterType.MALFORMED).increment(1); 
        } else {
            String[] stringElements = line.split("[:,]"); // бьем строку на [2022-01-01 00, 00, 00, 1]
            String codeName = "";
            MapWritable codeDict = new MapWritable();
            if (!stringElements[0].isEmpty() & !stringElements[stringElements.length - 1].isEmpty()) { // проверяем, что нужные нам элементы не пустые
                dateAndHour.set(stringElements[0]); // устанавливаем ключ записи
                indexOfCode = Integer.parseInt(stringElements[stringElements.length - 1]);
                codeName = codeNames[indexOfCode]; // get code name: crit, alert, etc.
                codeNameKey.set(codeName); // достаем наименование сообщения
                codeDict.put(codeNameKey, quantityOne); // в значение записи в виде справочника устанавливаем наименовние сообщения в виде ключа и единицу как value
                context.write(dateAndHour, codeDict);
            }
        }
    }
}
