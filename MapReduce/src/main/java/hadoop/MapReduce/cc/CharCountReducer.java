package hadoop.MapReduce.cc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 리듀스 역할을 수행하기 위해 Reducer 자바 파일을 상속받아야 한다.
 * Reducer 파일의 앞 2개 데이터 타입은 Shffle and Sort에 보낸 데이터의 키와 값의 데이터 타입이다.
 * 보통 Mapper에서 보낸 데이터타입과 동일하다.
 * Reducer 파일의 뒤 2개 데이터 타입은 결과 파일 생성에 사용할 키와 값이다.
 */
public class CharCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
        int wordCount = 0;

        // Suffle and Sort로 인해 단어별로 데이터들의 값들이 List 구조로 저장된다.
        // 오주현 : {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}
        // 모든 값은 1이기에 모두 더하기 해도 된다.
        for (IntWritable value : values) {
            //값을 모두 더한다.
            wordCount += value.get();
        }

        // 분석 결과 파일에 데이터를 저장한다.
        context.write(key, new IntWritable(wordCount));
    }
}
