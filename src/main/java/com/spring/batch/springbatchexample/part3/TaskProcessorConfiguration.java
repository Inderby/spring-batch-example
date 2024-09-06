package com.spring.batch.springbatchexample.part3;

import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@Slf4j
public class TaskProcessorConfiguration {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;
    private final EntityManagerFactory entityManager;

    public TaskProcessorConfiguration(JobRepository jobRepository, PlatformTransactionManager transactionManager, DataSource dataSource, EntityManagerFactory entityManager) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.dataSource = dataSource;
        this.entityManager = entityManager;
    }

    @Bean
    public Job taskProcessorJob() throws Exception {
        return new JobBuilder("taskProcessorJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(this.taskProcessorStep(null))
                .listener(new SavePersonListener.SavePersonJobExecutionListener())
                .listener(new SavePersonListener.SavePersonAnnotationJobExecutionListener())
                .build();
    }

    @Bean
    @JobScope
    public Step taskProcessorStep(@Value("#{jobParameters[allow_duplicate]}") String allowDuplicate) throws Exception {
        return new StepBuilder("taskProcessorStep", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(itemReader())
                .processor(new SavePersonProcessor<>(Boolean.parseBoolean(allowDuplicate), Person::getName))
                .writer(composedItemWriter())
                .listener(new SavePersonListener.SavePersonAnnotationStepExecutionListener())
                .faultTolerant()
                .skip(NotFoundNameException.class)
                .skipLimit(3)
                .retry(NullPointerException.class)
                .retryLimit(3)
                .build();

    }

    private ItemWriter<Person> composedItemWriter() throws Exception {

        ItemWriter<Person> saveItemWriter = new JpaItemWriterBuilder<Person>()
                .entityManagerFactory(entityManager)
                .build();

        ItemWriter<Person> logItemWriter = items -> log.info("person.size : {}", items.size());

        CompositeItemWriter<Person> itemWriter = new CompositeItemWriterBuilder<Person>()
                .delegates(saveItemWriter, logItemWriter)
                .build();

        itemWriter.afterPropertiesSet();
        return itemWriter;
    }

//    private ItemProcessor<? super Person,? extends Person> itemProcessor(@Value("") boolean allowDuplicate) {
//        Map<String, Person> map = new HashMap<>();
//        return item ->{
//            if(allowDuplicate) {
//                return item;
//            }else{
//                if(map.containsKey(item.getName())) {
//                    return null;
//                }else{
//                    map.put(item.getName(), item);
//                    return item;
//                }
//            }
//        };
//    }

    private ItemReader<Person> itemReader() throws Exception {
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "name", "age", "address");
        tokenizer.setDelimiter(",");
        lineMapper.setLineTokenizer(tokenizer);

        lineMapper.setFieldSetMapper(fieldSet -> {
            int id = fieldSet.readInt("id");
            String name = fieldSet.readString("name");
            String age = fieldSet.readString("age");
            String address = fieldSet.readString("address");
            return new Person(id, name, age, address);
        });

        FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .encoding("UTF-8")
                .resource(new ClassPathResource("test-output.csv"))
                .lineMapper(lineMapper)
                .build();

        itemReader.afterPropertiesSet();
        return itemReader;
    }
}
