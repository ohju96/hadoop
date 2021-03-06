package hadoop.MapReduce.ip;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IPCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 분석할 파일의 한 줄 값
        String line = value.toString();

        String ip = "";
        int forCnt = 0;


        for (String word : line.split("\\W+")) {

            if (word.length() > 0) {

                forCnt++;
                ip += (word + ".");

                if (forCnt == 4) {

                    // ip변수 값은 196.169.0.127. 처럼 마지막에도 .이 붙는다.
                    // 마지막 .을 제거하기 위해 0부터 마지막 위치에서 -1 값 까지 문자열을 짤라준다.
                    ip = ip.substring(0, ip.length()-1);

                    // Suffle and Sort로 데이터를 전달하기
                    // 전달하는 값은 단어와 빈도수(1)를 전달한다.
                    context.write(new Text(ip), new IntWritable(1));
                }
            }
        }
    }
}
