package hadoop.MapReduce.ip;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IPCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 부모 Reducer 자바 파일에 작성된 reduce 함수를 덮어쓰기 수행
     * reduce 함수는 Suffle and Sort로 처리된 데이터마다 실행된다.
     * 처리된 데이터 수가 500개라면, reduce 함수는 500번 실행된다.
     *
     * Reducer 객체는 기본값이 1개로 1개의 쓰레드로 처리한다.
     */

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        // 단어별 빈도수를 계산하기 위한 변수
        int ipCount = 0;

        // Suffle and Sort로 인해 단어별로 데이터들의 값들이 List 구조로 저장된다.
        // 오주현 : {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}
        // 모든 값은 1이기에 모두 더하기 해도 된다.
        for (IntWritable value : values) {
            //값을 모두 더한다.
            ipCount += value.get();
        }

        // 분석 결과 파일에 데이터를 저장한다.
        context.write(key, new IntWritable(ipCount));
    }
}
