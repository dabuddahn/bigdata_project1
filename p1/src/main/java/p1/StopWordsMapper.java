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
 * Created by dabuddahn on 03-23-17.
 */
public class StopWordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String keywords = status.getText().toUpperCase();
            String[] keywords_split = keywords.split("\\s+");

            for (String e : keywords_split) {
                if (e == "A") {
                } else if (e == "AN") {
                } else if (e == "AND") {
                } else if (e == "ARE") {
                } else if (e == "AS") {
                } else if (e == "BE") {
                } else if (e == "AT") {
                } else if (e == "BY") {
                } else if (e == "FOR") {
                } else if (e == "FROM") {
                } else if (e == "HAS") {
                } else if (e == "HE") {
                } else if (e == "IN") {
                } else if (e == "IS") {
                } else if (e == "IT") {
                } else if (e == "ITS") {
                } else if (e == "OF") {
                } else if (e == "ON") {
                } else if (e == "THAT") {
                } else if (e == "THE") {
                } else if (e == "TO") {
                } else if (e == "WAS") {
                } else if (e == "WERE") {
                } else if (e == "WILL") {
                } else if (e == "WITH") {
                } else {
                    //System.out.println(e);
                    context.write(new Text(e.replace("\"", "")), new IntWritable(1));
                }
            }
        } catch (TwitterException e) {

        }
    }
}