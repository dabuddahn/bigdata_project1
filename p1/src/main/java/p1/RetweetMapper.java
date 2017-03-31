package p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

/**
 * Created by dabuddahn on 03-26-17.
 */
public class RetweetMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            long status_id = status.getId();
            String strLong = Long.toString(status_id);

            if (status.isRetweet()) {
                System.out.println(status.isRetweet());

                Status retweeted_status = status.getRetweetedStatus();
                long retweet_id_key = retweeted_status.getId();

                int retweet_count = retweeted_status.getRetweetCount();
                System.out.println(retweet_count);

                context.write(new LongWritable(retweet_id_key), new IntWritable(retweet_count));
            }
        }

        catch(TwitterException e) {

        }
    }
}
