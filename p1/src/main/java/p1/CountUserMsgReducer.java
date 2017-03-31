package p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by dabuddahn on 03-26-17.
 */
public class CountUserMsgReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        String all_msg = " ";

        for (Text value : values){
            all_msg = all_msg.concat(value.toString()) + " ";
            count = count + 1;
            System.out.println(count);
        }

        String str_count = Integer.toString(count);
        all_msg = all_msg.concat("" + str_count);
     /**   Text key_output = null;
        String temp_key = key.toString();
        temp_key = temp_key.concat(all_msg);
        key_output.set(temp_key); **/

        context.write(key, new Text(all_msg));
    }
}

