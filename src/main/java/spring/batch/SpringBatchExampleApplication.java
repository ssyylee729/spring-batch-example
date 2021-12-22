package spring.batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

@SpringBootApplication
@EnableBatchProcessing // 이 application은 batch processing을 하겠다.
public class SpringBatchExampleApplication {

    public static void main(String[] args) {
        //async로 실행 시 완전히 종료가 안되는 경우가 있기 때문에 다음과 같이 batch 시행 후 안전히 종료되도록 함.
        System.exit(SpringApplication.exit(SpringApplication.run(SpringBatchExampleApplication.class, args)));
//        SpringApplication.run(SpringBatchExampleApplication.class, args);
    }
    //SpringBoot에선 taskExecutor bean이 기본적으로 설정되어 있기 때문에
    //생성된 애가 기본으로 동작하도록 @Primary 설정
    @Bean
    @Primary
    TaskExecutor taskExecutor(){
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        //pool 안에서 thread를 미리 생성해 놓고 필요할 때 사용하도록 함. 조금 더 효율적임.

        taskExecutor.setCorePoolSize(10); //기본 thread 수
        taskExecutor.setMaxPoolSize(20);
        taskExecutor.setThreadNamePrefix("batch-thread-");
        taskExecutor.initialize();
        return taskExecutor;
    }
}
