package com.spring.batch.springbatchexample.part3;

import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
@Slf4j
public class ItemReaderConfiguration {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;
    private final EntityManagerFactory entityManagerFactory;
    public ItemReaderConfiguration(JobRepository jobRepository, PlatformTransactionManager transactionManager, DataSource dataSource, EntityManagerFactory entityManagerFactory) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.dataSource = dataSource;
        this.entityManagerFactory = entityManagerFactory;
    }

    @Bean
    public Job itemReaderJob() throws Exception {
        return new JobBuilder("itemReaderJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(this.customItemReaderStep())
                .next(csvFileStep())
                .next(jdbcStep())
                .next(jpaStep())
                .build();
    }

    @Bean
    public Step customItemReaderStep() {
        return new StepBuilder("customItemReader", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(new CustomItemReader<>(getItems()))
                .writer(itemWriter())
                .build();

    }

    @Bean
    public Step csvFileStep() throws Exception {
        return new StepBuilder("csvFileStep", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(csvFileItemReader())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Step jdbcStep() throws Exception {
        return new StepBuilder("jdbcStep", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(jdbcItemReader())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Step jpaStep() throws Exception {
        return new StepBuilder("jpaStep", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(jpaItemReader())
                .writer(itemWriter())
                .build();
    }

    private JpaCursorItemReader<Person> jpaItemReader() throws Exception {
        JpaCursorItemReader<Person> itemReader = new JpaCursorItemReaderBuilder<Person>()
                .name("jpaItemReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select p from Person p") // JPQL query
                .build();
        itemReader.afterPropertiesSet();

        return itemReader;
    }


    private JdbcCursorItemReader<Person> jdbcItemReader() throws Exception {
        JdbcCursorItemReader<Person> itemReader = new JdbcCursorItemReaderBuilder<Person>()
                .name("jdbcCursorItemReader")
                .dataSource(dataSource)
                .sql("select * from person")
                .rowMapper(((rs, rowNum) -> new Person(
                        rs.getInt(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4)
                )))
                .build();
        itemReader.afterPropertiesSet();
        return itemReader;
    }

    @Bean
    public FlatFileItemReader<Person> csvFileItemReader() throws Exception {
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "name", "age", "address");
        lineMapper.setLineTokenizer(tokenizer);

        lineMapper.setFieldSetMapper(fieldSet -> {
            int id = fieldSet.readInt("id");
            String name = fieldSet.readString("name");
            String age = fieldSet.readString("age");
            String address = fieldSet.readString("address");
            return new Person(id, name, age, address);
        });

        FlatFileItemReader itemReader = new FlatFileItemReaderBuilder<Person>()
                .name("csvFileItemReader")
                .encoding("UTF-8")
                .resource(new ClassPathResource("test.csv"))
                .linesToSkip(1)
                .lineMapper(lineMapper)
                .build();
        itemReader.afterPropertiesSet(); // itemReader에 필요한 설정 값이 정상적으로 세팅 됐는지 확인하는 메서드
        return itemReader;
    }


    private ItemWriter<Person> itemWriter() {
        return items -> log.info(
                items.getItems()
                        .stream()
                        .map(Person::getName)
                        .collect(Collectors.joining(", "))
        );
    }
    private List<Person> getItems() {
        List<Person> items = new ArrayList<>();
        for(int i = 0; i < 10; i++){
                items.add(new Person(i + 1, "test name" + i, "test age", "test address"));
        }
        return items;
    }
}
