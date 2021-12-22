package spring.batch.part6;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
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
import java.util.concurrent.Future;

@Configuration
@Slf4j
public class AsyncUserConfiguration {
    private final String JOB_NAME = "asyncUserJob";
    private final int CHUNK = 1000;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final UserRepository userRepository;
    private final EntityManagerFactory entityManagerFactory;
    private final DataSource dataSource;
    //async item processor에서 사용할 taskExecutor 생성자로 주입 받음.
    private final TaskExecutor taskExecutor;

    public AsyncUserConfiguration(JobBuilderFactory jobBuilderFactory,
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
                .start(this.saveUserStep())
                .next(this. userLevelUpStep())
                .listener(new LevelUpJobExecutionListener(userRepository))
                .next(new JobParameterDecider("date"))
                .on(JobParameterDecider.CONTINUE.getName())
                .to(this.orderStatisticsStep(null))
                .build()
                .build();
    }
    @Bean(JOB_NAME+"_orderStatisticsStep")
    @JobScope // parameter 필요
    public Step orderStatisticsStep(@Value("#{jobParameters[date]}") String date) throws Exception {
        return stepBuilderFactory.get(JOB_NAME+"_orderStatisticsStep")
                .<OrderStatistics, OrderStatistics> chunk(CHUNK)
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

    @Bean(JOB_NAME+"_saveUserStep")
    public Step saveUserStep() {
        return stepBuilderFactory.get(JOB_NAME+"_saveUserStep")
                .tasklet(new SaveUserTasklet(userRepository))
                .build();
    }

    @Bean(JOB_NAME+"_userLevelUpStep")
    public Step userLevelUpStep() throws Exception {
        return stepBuilderFactory.get(JOB_NAME+"_userLevelUpStep")
                //Future로 감싼 item 을 processor에서는 output으로 제공하게 됨.
                .<User, Future<User>>chunk(CHUNK)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();

    }
    //async로 변경

    private AsyncItemWriter<User> itemWriter() {
        ItemWriter<User> itemWriter =  users -> { users.forEach(x-> {
                x.levelUp();
                userRepository.save(x);//update
            });
        };
        AsyncItemWriter<User> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(itemWriter);
        return asyncItemWriter;
    }

    //async로 변경
    private AsyncItemProcessor<User,User> itemProcessor() {
        //AS-WAS
        ItemProcessor<User, User> itemProcessor =  user -> {
            if(user.availableLevelUp()){
                return user;
            }
            return null;
        };
        //TO-BE
        //delegate 메소드로 기존 itemProcessor 감싸고, taskExecutor 주입 받음
        AsyncItemProcessor<User, User> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(itemProcessor);
        asyncItemProcessor.setTaskExecutor(this.taskExecutor);

        return asyncItemProcessor;
    }

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


}
