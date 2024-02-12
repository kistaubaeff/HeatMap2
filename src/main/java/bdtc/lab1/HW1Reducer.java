package bdtc.lab1;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Редьюсер: суммирует все единицы полученные от {@link HW1Mapper},
 * выдаёт суммарное количество пользователей по браузерам.
 */
public class HW1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static final int LOWLEFTBOUND = 0;

    private static final int LOWRIGHTBOUND = 100;

    private static final int MEDIUMLEFTBOUND = 101;

    private static final int MEDIUMRIGHTBOUND = 200;

    private static final int HIGHLEFTBOUND = 101;

    private static final int HIGHRIGHTBOUND = 200;


    /**
     * Sets up the ranges map from cache files or initializes default values.
     * @param context The context object containing cache files
     * @throws IOException throws IOException
     * @throws InterruptedException throws InterruptedException
     */
    @Override
    protected void setup(final Context context)
             throws IOException, InterruptedException {
        URI[] uris = context.getCacheFiles();
        if (uris != null && uris.length > 0) {
            Path ranges = new Path(uris[1].toString());
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ranges)))) {
                String line;
                line = br.readLine();
                while (line != null && line != "") {
                    String[] arr = line.split(" ");
                    int lowerBound = Integer.parseInt(arr[0]);
                    int upperBound = Integer.parseInt(arr[1]);
                    String value = arr[2];
                    CustomPair pair = new CustomPair(lowerBound, upperBound);
                    tempRange.put(value, pair);
                    line = br.readLine();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            tempRange.put("low", new CustomPair(LOWLEFTBOUND, LOWRIGHTBOUND));
            tempRange.put("medium", new CustomPair(MEDIUMLEFTBOUND, MEDIUMRIGHTBOUND));
            tempRange.put("high", new CustomPair(HIGHLEFTBOUND, HIGHRIGHTBOUND));
        }

    }
    /**
    * Represents a pair of integer values.
    */
    public final class CustomPair {
        private int key1;
        private int key2;

        /**
         * Constructs a new pair with the specified keys.
         *
         * @param key1 The first key.
         * @param key2 The second key.
         */
        public CustomPair(final int key1,
                        final int key2) {
            this.key1 = key1;
            this.key2 = key2;
        }

        /**
         * Gets the value of the first key.
         *
         * @return The value of the first key.
         */
        public int getKey1() {
            return key1;
        }

        /**
         * Sets the value of the first key.
         *
         * @param key1 The new value of the first key.
         */
        public void setKey1(final int key1) {
            this.key1 = key1;
        }

        /**
         * Gets the value of the second key.
         *
         * @return The value of the second key.
         */
        public int getKey2() {
            return key2;
        }

        /**
         * Sets the value of the second key.
         *
         * @param key2 The new value of the second key.
         */
        public void setKey2(final int key2) {
            this.key2 = key2;
        }
    }


    private Map<String, CustomPair> tempRange = new HashMap<>();

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
        String temperature = "unknown";
        for (String it : tempRange.keySet())
        {
            CustomPair current = tempRange.get(it);
            if (sum >= current.getKey1() && sum <= current.getKey2()) {
                temperature = it;
            }
        } 

        Text result = new Text(new StringBuilder()
                                    .append(key.toString())
                                    .append(",")
                                    .append(temperature)
                                    .toString());
        context.write(result, new IntWritable(sum));
    }
}
