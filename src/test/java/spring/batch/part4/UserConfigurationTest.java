package spring.batch.part4;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import spring.batch.TestConfiguration;

import java.time.LocalDate;

@SpringBatchTest
@ContextConfiguration(classes = {UserConfiguration.class, TestConfiguration.class})
class UserConfigurationTest {
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private UserRepository userRepository;

    @Test
    public void test() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        //오늘 날짜 기준으로 업데이트 된 user조회
        int size = userRepository.findAllByUpdatedDate(LocalDate.now()).size();
        //batch가 오늘을 걸쳐서 내일 끝난다면 조회가 되지 않으므로 데이터 검증 오류 생길 수 있음.
        //1. 즉, 일찍 시작한다고 해도 데이터 수 많아지면 언제 끝날지 보장 안됨.
        //2. updatedDate가 Level안에서만 수정된다는 보장이 없음. 요구사항 추가되면서 추가로 해당 필드가 수정될 가능성이 있음.


        Assertions.assertThat(jobExecution.getStepExecutions().stream()
                .filter(x-> x.getStepName().equals("userLevelUpStep"))
                .mapToInt(StepExecution::getWriteCount)
                .sum())
                .isEqualTo(size)
                .isEqualTo(300);

        Assertions.assertThat(userRepository.count())
                .isEqualTo(400);
    }

}