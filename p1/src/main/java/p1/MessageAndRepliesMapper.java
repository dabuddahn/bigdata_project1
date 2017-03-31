package p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

import java.io.IOException;

/**
 * Created by dabuddahn on 03-26-17.
 */
public class MessageAndRepliesMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            long replying_msg_id = status.getId();
            long msg_replied_to_id = status.getInReplyToUserId();

            if(msg_replied_to_id != -1){
                String str_replying_msg_id;
                str_replying_msg_id = Long.toString(replying_msg_id);

                System.out.println("Key: " + msg_replied_to_id);
              /**  System.out.println("Value: " + str_replying_msg_id); **/

                context.write(new LongWritable(msg_replied_to_id), new Text(str_replying_msg_id));
            }
        }

        catch(TwitterException e) {

        }
    }
}
