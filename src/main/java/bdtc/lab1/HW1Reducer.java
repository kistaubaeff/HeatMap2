package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Редьюсер: суммирует все единицы полученные от {@link HW1Mapper},
 * выдаёт суммарное количество пользователей по браузерам.
 */
public class HW1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Reduce function counts amount of each word.
     * @param key Key
     * @param values Values
     * @param context Hadoop Reducer context
     */
    @Override
    protected void reduce(
        final Text key,
        final Iterable<IntWritable> values,
        final Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
        }
        context.write(key, new IntWritable(sum));
    }
}
