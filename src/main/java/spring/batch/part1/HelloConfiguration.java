package spring.batch.part1;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration // 설정 class임을 인지하기 위하여
@Slf4j

/**
 * 설정 안해주면 모든 job이 실행되기 때문에 어떤 job을 실행할지에 대한 설정 필요
 * edit configuration에서 --spring.batch.job.names=helloJob 와 같이 argument 지정
 *
 * cf.application.yml에서 spring:batch:job:names 를 job.name으로 custom하게 설정.
 * 이렇게 설정하면 job.name이라는 파라미터로 job 실행할 수 있게 되며, 모든 batch가 실행되는 것을 막을 수 있다.
 * edit configuration에서 --job.name=helloJob 와 같이 argument 지정
 * */

public class HelloConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;


    public HelloConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory){
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    //Job은 batch의 단위이며 job을 만들 수 있도록 JobBuilderFactory 라는 클래스 제공
    @Bean
    public Job helloJob(){
        return jobBuilderFactory.get("helloJob") //job의 이름
                .incrementer(new RunIdIncrementer()) //실행단위를 구분할 수 있으며 job이 생성될 때마다 파라미터 아이디를 자동 생성해줌
                .start(this.helloStep()) //최초로 실행될 동작 설정
                .build();
    }
    //Step은 Job의 실행단위로, Job은 하나 이상의 Step 가질 수 있음.
    //Step 도 Job처럼 Bean으로 생성해줌.
    @Bean
    public Step helloStep(){
        return stepBuilderFactory.get("helloStep") //step 이름 설정
                .tasklet((contribution, chunkContext) -> {
                    log.info("hello spring batch");
                    return RepeatStatus.FINISHED;
                }).build();
    }
}
