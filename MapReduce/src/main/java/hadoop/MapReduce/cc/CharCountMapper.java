package hadoop.MapReduce.cc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 웹 역할을 수행하기 위해 Mapper 자바 파일을 상속 받는다.
 * Mapper 파일의 앞 2개 데이터 타입 LongWritable, Text 은 분석할 파일의 키와 값의 데이터 타입, FileInputFormat 객체로 분석팔 파일을 사용하면 무조건 LongWritable, Text을 사용한다. 레코드 리더가 무조건 이 구조로 전달을 해주기 때문이다.
 * Mapper 파일의 뒤 2개 데이터 타입 Text, IntWritable 은 리듀스에 보낼 키와 값의 데이터 타입, Shuffle and sort에 값을 전달할 데이터 타입
 */
public class CharCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 부모 Mapper 자바 파일에 작성된 map 함수를 덮어쓰기 수행
     * map 함수는 분석할 파일의 레코드 1줄마다 실행된다.
     * 파일의 라인수가 100개라면, map 함수는 100번 실행된다.
     */

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 분석할 파일의 한 줄 값
        String line = value.toString();

        // 단어 빈도수 구현은 단어가 아닌 것을 기준으로 단어로 구분한다.
        // 분석할 한 줄 내용을 단어가 아닌 것으로 나눈다.
        // word 변수는 단어가 저장된다.

        for (String word : line.split("\\W+")) {

            // word의 글자가 3글자 이상인 단어만 빈도수 세기
            if (word.length() > 3) {

                 // 대소문자 구분을 없애기 위해 모두 소문자로 변경
                word = word.toLowerCase();

                // Suffle and Sort로 데이터를 전달
                // 전달하는 값은 단어와 빈도수(1)를 전달한다.
                context.write(new Text(word), new IntWritable(1));
            }

        }
    }
}
