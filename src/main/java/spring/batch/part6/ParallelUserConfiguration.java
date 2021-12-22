package spring.batch.part6;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import spring.batch.part4.LevelUpJobExecutionListener;
import spring.batch.part4.SaveUserTasklet;
import spring.batch.part4.User;
import spring.batch.part4.UserRepository;
import spring.batch.part5.JobParameterDecider;
import spring.batch.part5.OrderStatistics;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class ParallelUserConfiguration {
    private final String JOB_NAME = "parallelUserJob";
    private final int CHUNK = 1000;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final UserRepository userRepository;
    private final EntityManagerFactory entityManagerFactory;
    private final DataSource dataSource;
    private final TaskExecutor taskExecutor;

    public ParallelUserConfiguration(JobBuilderFactory jobBuilderFactory,
                                     StepBuilderFactory stepBuilderFactory, UserRepository userRepository, EntityManagerFactory entityManagerFactory, DataSource dataSource, TaskExecutor taskExecutor) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.userRepository = userRepository;
        this.entityManagerFactory = entityManagerFactory;
        this.dataSource = dataSource;
        this.taskExecutor = taskExecutor;
    }

    @Bean(JOB_NAME)
    public Job userJob() throws Exception {
        return jobBuilderFactory.get(JOB_NAME)
                .incrementer(new RunIdIncrementer())
                .listener(new LevelUpJobExecutionListener(userRepository))
                .start(this.saveUserFlow())
                .next(this.splitFlow(null))
                .build()
                .build();
    }

    @Bean(JOB_NAME + "_saveUserFlow")
    public Flow saveUserFlow() {
        //기존 saveUserStep을 Flow로 감싸서 return
        TaskletStep saveUserStep = this.stepBuilderFactory.get(JOB_NAME + "_saveUserStep")
                .tasklet(new SaveUserTasklet(userRepository))
                .build();

        return new FlowBuilder<SimpleFlow>(JOB_NAME + "_saveUserFlow")
                .start(saveUserStep)
                .build();
    }

    @Bean(JOB_NAME + "_splitFlow")
    @JobScope
    //_userLevelUpFlow와 orderStatisticsFlow를 하나의 flow로 만드는 과정
    public Flow splitFlow(@Value("#{jobParameters[date]}") String date) throws Exception {
        Flow userLevelUpFlow = new FlowBuilder<SimpleFlow>(JOB_NAME + "_userLevelUpFlow")
//                .start(userLevelUpStep())
                .start(userLevelUpManagerStep())

                .build();

        //각각의 step이 flow여야 합쳐서 하나의 flow로 만들 수가 있음.
        return new FlowBuilder<SimpleFlow>(JOB_NAME + "_splitFlow")
                .split(this.taskExecutor)
                .add(userLevelUpFlow, orderStatisticsFlow(date))
                .build();

    }


    private Flow orderStatisticsFlow(String date) throws Exception {
        return new FlowBuilder<SimpleFlow>(JOB_NAME + "_orderStatisticsFlow")
                .start(new JobParameterDecider("date"))
                .on(JobParameterDecider.CONTINUE.getName())
                .to(this.orderStatisticsStep(date))
                .build();
    }

    private Step orderStatisticsStep(@Value("#{jobParameters[date]}") String date) throws Exception {
        return this.stepBuilderFactory.get(JOB_NAME + "_orderStatisticsStep")
                .<OrderStatistics, OrderStatistics>chunk(CHUNK)
                .reader(orderStatisticsItemReader(date))
                .writer(orderStatisticsItemWriter(date))
                .build();
    }


    private ItemWriter<? super OrderStatistics> orderStatisticsItemWriter(String date) throws Exception {
        //요구사항 - 읽은 결과를 바탕으로 파일 생성
        YearMonth yearMonth = YearMonth.parse(date);
        String fileName = yearMonth.getYear() + "년_" + yearMonth.getMonthValue() + "월_일별_주문_금액.csv";

        //mapping 설정 - OrderStatistics의 두 개 필드
        BeanWrapperFieldExtractor<OrderStatistics> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"amount", "date"});

        DelimitedLineAggregator<OrderStatistics> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);
        //FlatFileItemWriter은 chunk 반복 때마다 파일을 새로 생성하는 것이 아님
        //append 내용이 없더라도 리스트에 담아두었다가 한 파일에 write하게 됨.
        FlatFileItemWriter<OrderStatistics> itemWriter = new FlatFileItemWriterBuilder<OrderStatistics>()
                .resource(new FileSystemResource("output/" + fileName))
                .lineAggregator(lineAggregator)
                .name(JOB_NAME+"_orderStatisticsItemWriter")
                .encoding("UTF-8")
                .headerCallback(writer -> writer.write("total_amount,date"))
                .build();
        itemWriter.afterPropertiesSet();

        return itemWriter;
    }

    private ItemReader<? extends OrderStatistics> orderStatisticsItemReader(String date) throws Exception {
        YearMonth yearMonth = YearMonth.parse(date);

        Map<String, Object> params = new HashMap<>();
        //입력 받은 date 기준으로 해당 month의 며칠인지 지정
        params.put("startDate", yearMonth.atDay(1));
        //입력 받은 date 기준으로 해당 month의 마지막 날로 지정
        params.put("endDate", yearMonth.atEndOfMonth());

        //sort key 지정, Order 는 import org.springframework.batch.item.database.Order;
        Map<String, Order> sortKey = new HashMap<>();
        sortKey.put("created_date", Order.ASCENDING);

       JdbcPagingItemReader<OrderStatistics> itemReader = new JdbcPagingItemReaderBuilder<OrderStatistics>()
                .dataSource(this.dataSource)
               //읽어서 OrderStatistics에 mapping
                .rowMapper((resultSet, i) -> OrderStatistics.builder()
                        .amount(resultSet.getString(1))
                        .date(LocalDate.parse(resultSet.getString(2), DateTimeFormatter.ISO_DATE))
                        .build())
                .pageSize(CHUNK)
                .name(JOB_NAME+"_orderStatisticsItemReader")
                .selectClause("sum(amount), created_date")
                .fromClause("orders")
                .whereClause("created_date >= :startDate and created_date <= :endDate")
                .groupClause("created_date")
                .parameterValues(params)
                .sortKeys(sortKey)
                .build();

        itemReader.afterPropertiesSet();
        return itemReader;
    }
/*
    @Bean(JOB_NAME+"_saveUserStep")
    public Step saveUserStep() {
        return stepBuilderFactory.get(JOB_NAME+"_saveUserStep")
                .tasklet(new SaveUserTasklet(userRepository))
                .build();
    }
*/
    @Bean(JOB_NAME+"_userLevelUpStep")
    public Step userLevelUpStep() throws Exception {
        return stepBuilderFactory.get(JOB_NAME+"_userLevelUpStep")
                .<User, User>chunk(CHUNK)
                .reader(itemReader(null, null))
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();

    }

    private ItemWriter<? super User> itemWriter() {
        return users -> { users.forEach(x-> {
                x.levelUp();
                userRepository.save(x);//update
            });
        };
    }

    private ItemProcessor<? super User,? extends User> itemProcessor() {
        return user -> {
            if(user.availableLevelUp()){
                return user;
            }
            return null;
        };
    }
/*
    private ItemReader<? extends User> itemReader() throws Exception {
        //JpaPagingItemReader
        JpaPagingItemReader itemReader = new JpaPagingItemReaderBuilder<User>()
                .name(JOB_NAME+ "_jpaPagingItemReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select u from User u")
                .pageSize(CHUNK) //보통 chunk size와 동일하게 설정함.
                .build();
        itemReader.afterPropertiesSet();
        return itemReader;
    }
    */
    @Bean(JOB_NAME + "_userLevelUpStep.manager")
    public Step userLevelUpManagerStep() throws Exception {
    return this.stepBuilderFactory.get(JOB_NAME + "_userLevelUpStep.manager")
            .partitioner(JOB_NAME + "_userLevelUpStep", new UserLevelUpPartitioner(userRepository))
            .step(userLevelUpStep())
            .partitionHandler(taskExecutorPartitionHandler())
            .build();
}

    @Bean(JOB_NAME + "_taskExecutorPartitionHandler")
    PartitionHandler taskExecutorPartitionHandler() throws Exception {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(userLevelUpStep());
        handler.setTaskExecutor(this.taskExecutor);
        handler.setGridSize(8);

        return handler;
    }
    @Bean(JOB_NAME+ "_jpaPagingItemReader")
    @StepScope
    JpaPagingItemReader<? extends User> itemReader(@Value("#{stepExecutionContext[minId]}") Long minId
            , @Value("#{stepExecutionContext[maxId]}") Long maxId) throws Exception {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("minId", minId);
        parameters.put("maxId", maxId);
        //JpaPagingItemReader
        JpaPagingItemReader itemReader = new JpaPagingItemReaderBuilder<User>()
                .name(JOB_NAME+ "_jpaPagingItemReader")
                .parameterValues(parameters)
                .entityManagerFactory(entityManagerFactory)
                .queryString("select u from User u where u.id between :minId and :maxId")
                .pageSize(CHUNK) //보통 chunk size와 동일하게 설정함.
                .build();
        itemReader.afterPropertiesSet();
        return itemReader;
    }

}
