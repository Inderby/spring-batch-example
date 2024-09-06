package com.spring.batch.springbatchexample.part3;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.support.RetryTemplateBuilder;

public class PersonValidationRetryProcessor implements ItemProcessor<Person, Person> {
    private final RetryTemplate retryTemplate;

    public PersonValidationRetryProcessor() {
        this.retryTemplate = new RetryTemplateBuilder()
                .retryOn(NotFoundNameException.class)
                .withListener(new SavePersonListener.SavePersonRetryListener())
                .build();
    }

    @Override
    public Person process(Person item) throws Exception {
        return this.retryTemplate.execute(context -> {
            //retry callback
            if(item.isNotEmptyName())
                return item;

            throw new NotFoundNameException();
        }, context -> {
            //recovery callback
            return item.unknownName();
        });
    }
}
