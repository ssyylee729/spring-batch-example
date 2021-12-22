package spring.batch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Configuration
@Slf4j
public class SavePersonBatch {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private final Map<String, Person> map = new ConcurrentHashMap<>();

    public SavePersonBatch(JobBuilderFactory jobBuilderFactory,
                           StepBuilderFactory stepBuilderFactory, EntityManagerFactory entityManagerFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.entityManagerFactory = entityManagerFactory;
    }

    @Bean
    public Job hwJob() throws Exception {
        return jobBuilderFactory.get("hwJob")
                .incrementer(new RunIdIncrementer())
                .start(this.hwStep(null))
                .listener(new SavePersonListener.SavePersonJobExecutionListener())
                .listener(new SavePersonListener.SavePersonAnnotationJobExecutionListener())
                .build();
    }

    @Bean
    @JobScope
    public Step hwStep(@Value("#{jobParameters[allow_duplicate]}") String allow_duplicate) throws Exception {
        return stepBuilderFactory.get("hwStep")
                .<Person, Person>chunk(10)
                .reader(csvItemReader())
                .processor(itemProcessor(Boolean.parseBoolean(allow_duplicate)))
                .writer(itemWriter())
                //추가 21.11.25
                .listener(new SavePersonListener.SavePersonStepExecutionListener())
                .faultTolerant()// skip과 같은 예외처리 가능한 메서드 제공
                .skip(NotFoundNameException.class)
                .skipLimit(3) //3번까지 NotFoundNameException 허용한다는 의미
                .retry(NotFoundNameException.class)
                .retryLimit(3)
                .build();
    }

    private ItemReader<Person> csvItemReader() throws Exception {
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("name", "age", "address");
        lineMapper.setLineTokenizer(tokenizer);

        lineMapper.setFieldSetMapper(fieldSet -> {
            return new Person(fieldSet.readString(0), fieldSet.readString(1), fieldSet.readString(2));
        });
        FlatFileItemReader itemReader = new FlatFileItemReaderBuilder<Person>()
                .name("cvsItemReader")
                .encoding("UTF-8")
                .resource(new ClassPathResource("testPeople.csv"))
                .linesToSkip(1)
                .lineMapper(lineMapper)
                .build();
       itemReader.afterPropertiesSet();

       return itemReader;
    }
/*
    private ItemProcessor<Person, Person> itemProcessor(Boolean allow_duplicate){

            return item -> {
                if(!allow_duplicate) { //false인 경우: 겹치면 안됨.
                    if (map.containsKey(item.getName())) return null;
                    map.put(item.getName(), item);
                    return item;
                }
                else return item;
            };
        }
*/
    private ItemProcessor<Person, Person> itemProcessor(Boolean allow_duplicate) throws Exception {
        ItemProcessor<Person, Person> iProcessor1 = item -> {
            if(!allow_duplicate) { //false인 경우: 겹치면 안됨.
                if (map.containsKey(item.getName())) return null;
                map.put(item.getName(), item);
                return item;
            }
            else return item;
        };
        ItemProcessor<Person, Person> iProcessor2 = item -> {
            if(item.isNotEmptyName()){
                return item;
            }
            throw new NotFoundNameException();
        };
        CompositeItemProcessor<Person, Person> itemProcessor = new CompositeItemProcessorBuilder()
                .delegates(new PersonValidationRetryProcessor(), iProcessor2, iProcessor1)
                .build();
        itemProcessor.afterPropertiesSet();
        return itemProcessor;
    }
    private ItemWriter<Person> itemWriter() throws Exception {
        JpaItemWriter<Person> jpaItemWriter = new JpaItemWriterBuilder<Person>()
                .entityManagerFactory(entityManagerFactory)
                .build();

        ItemWriter<Person> logItemWriter = items -> log.info("person.size : {}", items.size());

        CompositeItemWriter<Person> itemWriter = new CompositeItemWriterBuilder<Person>()
                .delegates(jpaItemWriter, logItemWriter)
                .build();

        itemWriter.afterPropertiesSet();
        return itemWriter;
    }

    private ItemWriter<Person> itemWriter2(){
        return items -> log.info(items.stream()
                .map(Person::getName)
                .collect(Collectors.joining(", ")));
    }
}
