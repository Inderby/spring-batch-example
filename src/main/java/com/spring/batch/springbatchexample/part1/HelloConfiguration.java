package com.spring.batch.springbatchexample.part1;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@Slf4j
public class HelloConfiguration {
//   TODO :  예전 코드
//    private final JobBuilderFactory jobBuilderFactory;
//    public HelloConfiguration(JobBuilderFactory jobBuilderFactory) {
//        this.jobBuilderFactory = jobBuilderFactory;
//    }

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public HelloConfiguration(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    @Bean
    public Job helloJob() {
        return new JobBuilder("helloJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(this.helloStep())
                .build();

    }

    @Bean
    public Step helloStep() {
        return new StepBuilder("helloStep", jobRepository)
                .tasklet(((contribution, chunkContext) -> {
                    log.info("Hello spring Batch");
                    return RepeatStatus.FINISHED;
                }), transactionManager)
                .build();
    }
}
