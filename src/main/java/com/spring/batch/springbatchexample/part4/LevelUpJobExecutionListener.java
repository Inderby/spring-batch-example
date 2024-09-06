package com.spring.batch.springbatchexample.part4;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.time.Duration;
import java.time.LocalDate;
import java.util.Collection;

@Slf4j
public class LevelUpJobExecutionListener implements JobExecutionListener {

    private final UserRepository userRepository;

    public LevelUpJobExecutionListener(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        JobExecutionListener.super.beforeJob(jobExecution);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        Collection<User> users = userRepository.findAll();

        long time = Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis();
        log.info("회원등급 배치 프로그램");
        log.info("-----------------");
        log.info("총 데이터 처리 {}건, 처리 시간 {}millis", users.size(), time);

        JobExecutionListener.super.afterJob(jobExecution);
    }
}
