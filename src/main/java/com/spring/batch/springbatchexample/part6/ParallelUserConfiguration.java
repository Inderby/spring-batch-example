package com.spring.batch.springbatchexample.part6;

import com.spring.batch.springbatchexample.part4.LevelUpJobExecutionListener;
import com.spring.batch.springbatchexample.part4.SaveUserTasklet;
import com.spring.batch.springbatchexample.part4.User;
import com.spring.batch.springbatchexample.part4.UserRepository;
import com.spring.batch.springbatchexample.part5.JobParametersDecide;
import com.spring.batch.springbatchexample.part5.OrderStatistics;
import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
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
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class ParallelUserConfiguration {

    private final UserRepository userRepository;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final EntityManagerFactory entityManagerFactory;
    private final DataSource dataSource;
    private final String JOB_NAME = "ParallelUserJob";
    private final TaskExecutor taskExecutor;

    public ParallelUserConfiguration(UserRepository userRepository, JobRepository jobRepository, PlatformTransactionManager transactionManager, EntityManagerFactory entityManagerFactory, DataSource dataSource, TaskExecutor taskExecutor) {
        this.userRepository = userRepository;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.entityManagerFactory = entityManagerFactory;
        this.dataSource = dataSource;
        this.taskExecutor = taskExecutor;
    }

    @Bean(JOB_NAME)
    public Job userJob() throws Exception {
        return new JobBuilder(JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(new LevelUpJobExecutionListener(userRepository))
                .start(saveUserFlow())
                .next(this.splitFlow(null))
                .build()
                .build();
    }

    @Bean(JOB_NAME + "_saveUserFlow")
    public Flow saveUserFlow() throws Exception {
        TaskletStep step = new StepBuilder(JOB_NAME + "_saveUserStep", jobRepository)
                .tasklet(new SaveUserTasklet(userRepository), transactionManager)
                .build();
        return new FlowBuilder<SimpleFlow>(JOB_NAME + "_saveUserFlow")
                .start(step)
                .build();
    }

    @Bean(JOB_NAME + "_splitFlow")
    @JobScope
    public Flow splitFlow(@Value("#{jobParameters[date]}") String date) throws Exception {
        Flow userLevelUpFlow = new FlowBuilder<SimpleFlow>(JOB_NAME + "_userLevelUpFlow")
                .start(userLevelUpManagerStep())
                .build();

        return new FlowBuilder<SimpleFlow>(JOB_NAME + "_splitFlow")
                .split(taskExecutor)
                .add(userLevelUpFlow, orderStatisticsFlow(date))
                .build();
    }

    private Flow orderStatisticsFlow(String date) throws Exception {
        return new FlowBuilder<SimpleFlow>(JOB_NAME + "_orderStatisticsFlow")
                .start(new JobParametersDecide("date"))
                .on(JobParametersDecide.CONTINUE.getName())
                .to(this.orderStatisticsStep(date))
                .build();

    }

    private Step orderStatisticsStep(@Value("#{jobParameters[date]}") String date) throws Exception {
        return new StepBuilder(JOB_NAME + "_orderStatisticsStep", jobRepository)
                .<OrderStatistics, OrderStatistics>chunk(1000, transactionManager)
                .reader(orderStatisticsItemReader(date))
                .writer(orderStatisticsWriter(date))
                .build();
    }

    private ItemWriter<? super OrderStatistics> orderStatisticsWriter(String date) throws Exception {
        YearMonth yearMonth = YearMonth.parse(date);
        String fileName = yearMonth.getYear() + "년" + yearMonth.getMonthValue() + "월_일별_주문_금액.csv";

        BeanWrapperFieldExtractor<OrderStatistics> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"amount", "date"});

        DelimitedLineAggregator<OrderStatistics> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        FlatFileItemWriter<OrderStatistics> itemWriter = new FlatFileItemWriterBuilder<OrderStatistics>()
                .resource(new FileSystemResource("output/" + fileName))
                .lineAggregator(lineAggregator)
                .name(JOB_NAME + "_orderStatisticsWriter")
                .encoding("UTF-8")
                .headerCallback(writer -> {
                    writer.write("total_amount, date");
                })
                .build();
        itemWriter.afterPropertiesSet();
        return itemWriter;
    }

    private ItemReader<? extends OrderStatistics> orderStatisticsItemReader(String date) throws Exception {
        YearMonth yearMonth = YearMonth.parse(date);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("startDate", yearMonth.atDay(1));
        parameters.put("endDate", yearMonth.atEndOfMonth());

        Map<String, Order> sortKey = new HashMap<>();
        sortKey.put("created_date", Order.ASCENDING);
        JdbcPagingItemReader<OrderStatistics> itemReader = new JdbcPagingItemReaderBuilder<OrderStatistics>()
                .dataSource(this.dataSource)
                .rowMapper((resultSet, i) -> OrderStatistics.builder()
                        .amount(resultSet.getString(1))
                        .date(LocalDate.parse(resultSet.getString(2), DateTimeFormatter.ISO_DATE))
                        .build())
                .pageSize(1000)
                .name(JOB_NAME + "_orderStatisticsItemReader")
                .selectClause("sum(amount), created_date")
                .fromClause("orders")
                .whereClause("created_date >= :startDate and created_date <= :endDate")
                .groupClause("created_date")
                .parameterValues(parameters)
                .sortKeys(sortKey)
                .build();
        itemReader.afterPropertiesSet();
        return itemReader;
    }

//    @Bean(JOB_NAME + "_saveUserStep")
//    public Step saveUserStep() {
//        return new StepBuilder(JOB_NAME + "_saveUserStep", jobRepository)
//                .tasklet(new SaveUserTasklet(userRepository), transactionManager)
//                .build();
//    }

    @Bean(JOB_NAME + "_userLevelUpStep")
    public Step userLevelUpStep() throws Exception {
        return new StepBuilder(JOB_NAME + "_userLevelUpStep", jobRepository)
                .<User,User>chunk(1000, transactionManager)
                .reader(itemReader(null, null))
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }



    private ItemWriter<? super User> itemWriter() {
        return users->
            users.forEach( user -> {
                user.levelUp();
                userRepository.save(user);
            });
    }

    private ItemProcessor<? super User, ? extends User> itemProcessor() {

        return user->{
            if(user.availableLevelUp()){
                return user;
            }

            return null;
        };
    }

    @Bean(JOB_NAME + "_userItemReader")
    @StepScope
    public JpaPagingItemReader<? extends User> itemReader(@Value("#{stepExecutionContext[minId]}") Long minId,
                                                          @Value("#{stepExecutionContext[maxId]}")Long maxId) throws Exception {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("minId", minId);
        parameters.put("maxId", maxId);
        JpaPagingItemReader<User> itemReader = new JpaPagingItemReaderBuilder<User>()
                .parameterValues(parameters)
                .queryString("select u from users u where u.id between :minId and :maxId")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(1000)
                .name(JOB_NAME + "_userItemReader")
                .build();
        itemReader.afterPropertiesSet();
        return itemReader;
    }

    @Bean(name = JOB_NAME + "_userLevelUpStep.manager")
    public Step userLevelUpManagerStep() throws Exception {
        return new StepBuilder(JOB_NAME + "_userLevelUpStep.manager", jobRepository)
                .partitioner(JOB_NAME + "_userLevelUp", new UserLevelUpPartitioner(userRepository))
                .step(userLevelUpStep())
                .partitionHandler(taskExecutorPartitionHandler())
                .build();
    }

    @Bean(name = JOB_NAME + "_taskExecutorPartitionHandler")
    public PartitionHandler taskExecutorPartitionHandler() throws Exception {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();

        handler.setStep(userLevelUpStep());
        handler.setTaskExecutor(taskExecutor);
        handler.setGridSize(8);
        return handler;
    }
}
