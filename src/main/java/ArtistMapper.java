import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ArtistMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split CSV
        String row = value.toString();
        String[] rowData = row.split(",");

        if(rowData[1] != null){
           String artistName = rowData[1].replaceAll("\"","");
           String outputKey = artistName.replaceAll(" ","_");
           context.write(new Text(outputKey), new IntWritable(1));
        }
    }
}
