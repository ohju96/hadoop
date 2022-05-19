package hadoop.MapReduce.seq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadCompressSequenceFile extends Configuration implements Tool {
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.exit(-1);
        }

        int exitCode = ToolRunner.run(new ReadCompressSequenceFile(), args);

        System.exit(exitCode);
    }

    @Override
    public void setConf(Configuration conf) {

        // App 이름 정의
        conf.set("AppName", "Compress Sequence File Read Test");

    }

    @Override
    public Configuration getConf() {

        //맵리듀스 전체에 적용될 변수를 정의할 때 사용
        Configuration conf = new Configuration();

        //변수 정의
        this.setConf(conf);

        return conf;
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        String appName = conf.get("AppName");

        System.out.println("appName = " + appName);

        // 맵리듀스 시랳ㅇ을 위한 잡 객체를 가져온다.
        // 하둡이 실행되면, 기본적으로 잡 객체를 메모리에 올린다.
        Job job = Job.getInstance(conf);

        // 맵리듀스 잡이 시작되는 main 함수가 존재하는 파일 설정
        job.setJarByClass(ReadCompressSequenceFile.class);

        // 맵리듀스 잡 이름 설정, 리소스 매니저 등 맵리듀스 실행 결과 및 로그 확인할 때 편리
        job.setJobName(appName);

        // 분석할 폴더(파일) -- 첫 번째 파라미터
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 분석 결과가 저장되는 폴더(파일) -- 두 번째 파라미터
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 압축된 시퀀스 파일 읽기 설정
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // 리듀서 객체를 생성하지 못 하도록 객체의 수를 0 으로 정의
        job.setNumReduceTasks(0);

        // 맵리듀스 실행
        boolean success = job.waitForCompletion(true);

        return (success ? 0 : 1);

    }
}