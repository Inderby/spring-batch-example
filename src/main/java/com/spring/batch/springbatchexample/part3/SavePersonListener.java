package com.spring.batch.springbatchexample.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeJob;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;

@Slf4j
public class SavePersonListener {

    public static class SavePersonRetryListener implements RetryListener {
        @Override
        public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
            return RetryListener.super.open(context, callback);
        }

        @Override
        public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            log.info("close");
            RetryListener.super.close(context, callback, throwable);
        }

        @Override
        public <T, E extends Throwable> void onSuccess(RetryContext context, RetryCallback<T, E> callback, T result) {

            RetryListener.super.onSuccess(context, callback, result);
        }

        @Override
        public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            log.info("onError");
            RetryListener.super.onError(context, callback, throwable);
        }
    }

    public static class SavePersonAnnotationStepExecutionListener{
        @BeforeStep
        public void beforeStep(StepExecution stepExecution) {
            log.info("Before step");
        }
        @AfterStep
        public ExitStatus afterStep(StepExecution stepExecution) {
            log.info("After step : {} ", stepExecution.getWriteCount());
            return stepExecution.getExitStatus();
        }
    }

    public static class SavePersonJobExecutionListener implements JobExecutionListener {
        @Override
        public void beforeJob(JobExecution jobExecution) {
            log.info("before job");
            JobExecutionListener.super.beforeJob(jobExecution);
        }

        @Override
        public void afterJob(JobExecution jobExecution) {
            long sum = jobExecution.getStepExecutions().stream()
                    .mapToLong(StepExecution::getWriteCount)
                    .sum();
            log.info("after job : {}", sum);
            JobExecutionListener.super.afterJob(jobExecution);
        }
    }

    public static class SavePersonAnnotationJobExecutionListener {
        @BeforeJob
        public void beforeJob(JobExecution jobExecution) {
            log.info("annotation before job");
        }

        @AfterJob
        public void afterJob(JobExecution jobExecution) {
            long sum = jobExecution.getStepExecutions().stream()
                    .mapToLong(StepExecution::getWriteCount)
                    .sum();
            log.info("annotation after job : {}", sum);
        }
    }
}
