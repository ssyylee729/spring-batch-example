package spring.batch.part3;

import io.micrometer.core.instrument.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import java.util.ArrayList;
import java.util.List;

@Configuration
@Slf4j
public class ChunkProcessingConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public ChunkProcessingConfiguration(JobBuilderFactory jobBuilderFactory,
                                        StepBuilderFactory stepBuilderFactory){
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job chunkProcessingJob(){
        return jobBuilderFactory.get("chunkProcessingJob")
                .incrementer(new RunIdIncrementer())
                .start(this.taskBaseStep())
                .next(this.chunkBaseStep(null))
                .build();

    }
    @Bean
    @JobScope
    public Step chunkBaseStep(@Value("#{jobParameters[chunkSize]}") String chunkSize){
        //10개씩 나누라는 의미, 100개인 경우 10번 실행
        return stepBuilderFactory.get("chunkBaseStep")
                .<String, String>chunk(StringUtils.isNotEmpty(chunkSize)? Integer.parseInt(chunkSize): 10) //input type, output type 둘다 String
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    private ItemReader<String> itemReader(){
        return new ListItemReader<>(getItems());
    }

    private ItemProcessor<String, String> itemProcessor(){
        return item -> item + ", Spring Batch";
    }

    private ItemWriter<String> itemWriter(){
        return items -> log.info("chunk item size: {}", items.size());
//        return items -> items.forEach(log::info);
    }
    @Bean
    public Step taskBaseStep(){
        return stepBuilderFactory.get("taskBaseStep")
                .tasklet(this.tasklet(null))
                .build();
    }

//    private Tasklet tasklet(){
//        return(contribution, chunkContext) -> {
//            List<String> items = getItems();
//            log.info("task item size: {}", items.size());
//
//            return RepeatStatus.FINISHED;
//        };
//    }

    //chunk처럼 10개 씩 나누어서 처리하도록 코드 수정
   /* private Tasklet tasklet(){
        List<String> items = getItems();

        return(contribution, chunkContext) -> {
            StepExecution stepExecution = contribution.getStepExecution();
            //stepExecution은 읽은 아이템 크기를 저장할 수 있음.

            int chunkSize = 10;
            int fromIndex = stepExecution.getReadCount(); // chunk에서 읽은 아이템 크기
            int toIndex = fromIndex + chunkSize;

            if(fromIndex >= items.size()){
                return RepeatStatus.FINISHED;
            }
            List<String> subList = items.subList(fromIndex, toIndex);
            log.info("task item size: {}", subList.size());
            stepExecution.setReadCount(toIndex);

            return RepeatStatus.CONTINUABLE; //tasklet을 반복해서 처리하라는 의미.
        };
    }*/

    //21.11.15 job param 추가
    //@StepScope 추가
    @Bean
    @StepScope//주석 처리 해주면 오류발생. jobParameters 접근하는 것이 StepScope Life cycle 통해서임을 알 수 있음
    //Bean 선언했으므로 private -> public 변경
    public Tasklet tasklet(@Value("#{jobParameters[chunkSize]}") String value){
        List<String> items = getItems();

        return(contribution, chunkContext) -> {
            StepExecution stepExecution = contribution.getStepExecution();
            //stepExecution은 읽은 아이템 크기를 저장할 수 있음.
            //21.11.15 job param 추가
//            JobParameters jobParameters = stepExecution.getJobParameters();
//            String value = jobParameters.getString("chunkSize", "10");
            int chunkSize = StringUtils.isNotEmpty(value)? Integer.parseInt(value) : 10;
            int fromIndex = stepExecution.getReadCount(); // chunk에서 읽은 아이템 크기
            int toIndex = fromIndex + chunkSize;

            if(fromIndex >= items.size()){
                return RepeatStatus.FINISHED;
            }
            List<String> subList = items.subList(fromIndex, toIndex);
            log.info("task item size: {}", subList.size());
            stepExecution.setReadCount(toIndex);

            return RepeatStatus.CONTINUABLE; //tasklet을 반복해서 처리하라는 의미.
        };
    }
    private List<String> getItems(){
        List<String> items = new ArrayList<>();
        for(int i=0; i<100; i++){
            items.add(i + " Hello");
        }
        return items;
    }
}
