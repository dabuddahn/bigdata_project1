package p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by dabuddahn on 03-26-17.
 */
public class MessageAndRepliesReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String all_IDs = " ";
        System.out.println("Key: " + key);
        for (Text value : values){
            System.out.println(value.toString());
            all_IDs = all_IDs.concat(value.toString()) + " ";
        }

        context.write(key, new Text(all_IDs));
    }
}
