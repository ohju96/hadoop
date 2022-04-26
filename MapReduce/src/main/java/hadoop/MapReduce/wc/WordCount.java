package hadoop.MapReduce.wc;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 맵리듀스를 실행하기 위한 Main 함수가 존재하는 자바 파일
 * 드라이버 파일로 부른다.
 */
public class WordCount {

    // 맵리듀스 실행 함수
    public static void main(String[] args) throws Exception {

        // 파라미터는 분석할 파일(폴더)과 분석 결과가 저장될 파일(폴더)로 2개 받는다.
        if (args.length != 2) {
            System.out.println("분석할 폴드(파일) 및 분석 결과가 저장될 폴더를 입력해야 합니다.");
            System.exit(-1);
        }

        // 맵리듀스 한 번 실행 할 것을 'Job'이라고 한다.
        // 맵리듀스 실행을 위한 잡 객체를 가져온다.
        // 하둡이 실행되면, 기본적으로 잡 객체를 메모리에 올린다.
        Job job = Job.getInstance();

        // 맵리듀스 잡이 시작되는 main 함수가 존재하는 파일 설정
        job.setJarByClass(WordCount.class);

        // 해도 되고 안 해도 상관은 없다. 이름 찾기 편하게 하는 편의 기능이다.
        // 맵리듀스 잡 이름 설정, 리소스 매니저 등 맵리듀스 실행 결과 및 로그 확인할 때 편함
        job.setJobName("Word Count");

        // FileInputFormat의 부모가 레코드 리더라고 보면 된다.
        // 분석할 폴더(파일) -- 첫 번째 파라미터
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 분석 결과가 저장되는 폴더(파일) -- 두 번째 파라미터
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 맵리듀스의 맵 역할을 수행하는 Mapper 자바 파일 설정
        job.setMapperClass(WordCountMapper.class);

        // 맵리듀스의 리듀스 역할을 수행하는 Reducer 자바 파일 설정
        job.setReducerClass(WordCountReducer.class);

        // 분석 결과가 저장될 때 사용될 키의 데이터 타입
        // 하둡용 String  == Text
        job.setOutputKeyClass(Text.class);

        // 분석 결과가 저장될 때 사용될 값의 데이터 타입
        // 하둡용 int == IntWritable
        job.setOutputValueClass(IntWritable.class);

        // 맵리듀스 실행
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);

    }
}
