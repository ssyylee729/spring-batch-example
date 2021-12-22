package spring.batch.part3;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.test.context.ContextConfiguration;

/**
 * 최근 스프링 부트는 JUnit 5를 사용하기 때문에 더이상 JUnit 4에서 제공하던 @RunWith를 쓸 필요가 없고 (쓰고 싶으면 쓸 수는 있지만),
 * @ExtendWith를 사용해야 하지만, 이미 스프링 부트가 제공하는 모든 테스트용 애노테이션에 메타 애노테이션으로 적용되어 있기 때문에
 * @ExtendWith(SpringExtension.class)를 생략할 수 있다.
 *
 * https://www.whiteship.me/springboot-no-more-runwith/*/

@SpringBatchTest //scope 이 동작하도록 하기 위하여
//@SpringBootTest //@RunWith 대체
//@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes ={SavePersonBatch.class, TestConfiguration.class})
class MyHomeworkBatchTest {

    @Autowired
    JobLauncherTestUtils jobLauncherTestUtils ; //테스트 코드에서 job과 step 실행이 가능하게끔 해주는 클래스 주입 받음.

    @Autowired
    PersonRepository personRepository;

    @AfterEach //version에 따라 @After
    public void tearDown() throws Exception {
        personRepository.deleteAll();
    }
    @Test
    public void test_step() {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("hwStep");

        Assertions.assertThat(jobExecution.getStepExecutions().stream()
                        .mapToInt(StepExecution::getWriteCount)
                        .sum())
                .isEqualTo(personRepository.count())
                .isEqualTo(5);
    }
    @Test
    void test_allow_duplicate() throws Exception {
        System.out.println("111:"+personRepository.count());
        //given
        JobParameters jobParameters= new JobParametersBuilder()
                .addString("allow_duplicate", "false")
                .toJobParameters();

        //when : test대상
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        //위에서 설정한 ContextConfiguration의 job이 launchJob method에 의해 실행되고 그 결과가 jobExecution에 저장

        //then
        Assertions.assertThat(jobExecution.getStepExecutions().stream() //step이 여러개면 여러 step 가져옴
                        .mapToInt(StepExecution::getWriteCount)
                        .sum())
                .isEqualTo(personRepository.count())
                .isEqualTo(5);

        System.out.println("222:"+personRepository.count());

    }
//    @Test
//    void test_not_allow_duplicate() throws Exception {
//        //given
//        System.out.println("333:"+personRepository.count());
//
//        JobParameters jobParameters= new JobParametersBuilder()
//                .addString("allow_duplicate", "true")
//                .toJobParameters();
//
//        //when : test대상
//        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
//        //위에서 설정한 ContextConfiguration의 job이 launchJob method에 의해 실행되고 그 결과가 jobExecution에 저장
//
//        //then
//        Assertions.assertThat(jobExecution.getStepExecutions().stream() //step이 여러개면 여러 step 가져옴
//                        .mapToInt(StepExecution::getWriteCount)
//                        .sum())
//                        .isEqualTo(personRepository.count())
//                        .isEqualTo(100);
//
//        System.out.println("444:"+personRepository.count());
//
//    }


}