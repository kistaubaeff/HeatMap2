package bdtc.lab1;



import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Hadoop mapper.
 */
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    private Map<String, String> areasMap = new HashMap<>();

    /**
     * Returns the region name for given coordinates (x, y) in the regions map.
     * @param x The x-coordinate
     * @param y The y-coordinate
     * @param regions A map of region names and their boundaries
     * @return The region name or "unknown" if the point is not within any region
     */
    public static String getRegion(int x, int y, Map<String, String> regions) {
        for (String regionName : regions.keySet()) {
            String[] points = regions.get(regionName).split(" ");
            int minX = Integer.parseInt(points[0]);
            int minY = Integer.parseInt(points[1]);
            int maxX = Integer.parseInt(points[2]);
            int maxY = Integer.parseInt(points[3]);

            if (x >= minX && x <= maxX && y >= minY && y <= maxY) {
                return regionName;
            }
        }
        return "unknown";
    }

    /**
     * Sets up the areas map from cache files or initializes default values.
     * @param context The context object containing cache files
     * @throws IOException throws IOException
     * @throws InterruptedException throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] uris = context.getCacheFiles();
        if (uris != null && uris.length > 0) {
            Path areas = new Path(uris[0].toString());
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(areas)))) {
                String line;
                line = br.readLine();
                while (line != null && line != "") {
                    String[] arr = line.split(" ");
                    areasMap.put(arr[0], line.substring(arr[0].length() + 1));
                    line = br.readLine();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {
            areasMap.put("lower_right", "500 0 1000 750");
            areasMap.put("upper_right", "500 750 1000 1500");
            areasMap.put("lower_left", "0 0 500 750");
            areasMap.put("upper_left", "0 750 500 1500");
        }
    }
    
    /**
     * Map function makes mapping by words.
     * @param key Keyt
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
        String[] info = line.split(" "); 
        int x = Integer.parseInt(info[0]);
        int y = Integer.parseInt(info[1]);
        String result = getRegion(x, y, areasMap);
        
        if (result == "unknown") {
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            word.set(result);
            context.write(word, ONE);
        }
    }
}
