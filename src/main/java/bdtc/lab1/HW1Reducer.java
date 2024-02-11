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
    
    /**
     * Reduce function counts amount of each word.
     * @param key Key
     * @param values Values
     * @param context Hadoop Reducer context
     */
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] uris = context.getCacheFiles();
        if (uris != null && uris.length > 0) {
            Path ranges = new Path(uris[1].toString());
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ranges)))) {
                String line;
                line = br.readLine();
                while (line != null && line != "") {
                    String[] arr = line.split(" ");
                    int lower_bound = Integer.parseInt(arr[0]);
                    int upper_bound = Integer.parseInt(arr[1]);
                    String value = arr[2];
                    CustomPair pair = new CustomPair(lower_bound, upper_bound);
                    tempRange.put(value, pair);
                    line = br.readLine();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
            tempRange.put("low", new CustomPair(0, 100));
            tempRange.put("medium", new CustomPair(101, 200));
            tempRange.put("high", new CustomPair(201, 300));
        }

    }

    public class CustomPair {
        private int key1;
        private int key2;

        public CustomPair(int key1, int key2) {
            this.key1 = key1;
            this.key2 = key2;
        }
        
        public int getKey1() {
            return key1;
        }
        public void setKey1(int key1) {
            this.key1 = key1;
        }
        public int getKey2() {
            return key2;
        }
        public void setKey2(int key2) {
            this.key2 = key2;
        }
    }

    private Map<String, CustomPair> tempRange = new HashMap<>();

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
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(key.toString());
        stringBuilder.append(",");
        stringBuilder.append(temperature);
        Text result = new Text(stringBuilder.toString());
        context.write(result, new IntWritable(sum));
        
    }
}
