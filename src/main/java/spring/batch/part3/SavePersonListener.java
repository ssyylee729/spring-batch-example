package spring.batch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeJob;
import org.springframework.batch.core.annotation.BeforeStep;


@Slf4j
public class SavePersonListener {
    public static class SavePersonStepExecutionListener {
        @BeforeStep
        public void beforeStep(StepExecution stepExecution) {
            log.info("beforeStep");
        }

        @AfterStep
        public ExitStatus afterStep(StepExecution stepExecution) { //반환타입이 void가 아님.
            log.info("afterStep : {}", stepExecution.getWriteCount());


            //Spring Batch는 내부적으로 step이 실패/종료되는 상태를 stepExecution에 저장함

            //이런식으로 사용자가 정의도 가능함.
            if(stepExecution.getWriteCount() == 0)
                return ExitStatus.FAILED;
            return stepExecution.getExitStatus();

        }
    }


    //  Listener 구현 1: 인터페이스 구현
    public static class SavePersonJobExecutionListener implements JobExecutionListener {

        @Override
        public void beforeJob(JobExecution jobExecution) {
            //job 실행 전에 호출되는 메서드
            log.info("beforeJob");
        }

        @Override
        public void afterJob(JobExecution jobExecution) {
            //job 실행 후에 호출되는 메서드
            int sum = jobExecution.getStepExecutions().stream()
                    .mapToInt(StepExecution::getWriteCount)
                    .sum();

            log.info("afterJob: {}", sum);
        }
    }

    // Listener 구현 2: @Annotation 정의

    public static class SavePersonAnnotationJobExecutionListener{
        @BeforeJob
        public void beforeJob(JobExecution jobExecution) {
            //job 실행 전에 호출되는 메서드
            log.info("annotion beforeJob");
        }

        @AfterJob
        public void afterJob(JobExecution jobExecution) {
            //job 실행 후에 호출되는 메서드
            int sum = jobExecution.getStepExecutions().stream()
                    .mapToInt(StepExecution::getWriteCount)
                    .sum();

            log.info("annotation afterJob: {}", sum);
        }
    }
}