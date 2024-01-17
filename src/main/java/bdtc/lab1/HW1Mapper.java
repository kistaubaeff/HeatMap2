package bdtc.lab1;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Hadoop mapper.
 */
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    /**
     * Map function makes mapping by words.
     * @param key Key
     * @param value Value
     * @param context Hadoop Mapper context
     */
    @Override
    protected void map(
        final LongWritable key,
        final Text value,
        final Context context
    ) throws IOException, InterruptedException {
        String line = value.toString();
        UserAgent userAgent = UserAgent.parseUserAgentString(line);
        if (userAgent.getBrowser() == Browser.UNKNOWN) {
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            word.set(userAgent.getBrowser().getName());
            context.write(word, ONE);
        }
    }
}
