package com.spring.batch.springbatchexample.part3;

import org.springframework.batch.item.ItemProcessor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class SavePersonProcessor <T> implements ItemProcessor<T, T> {
    private boolean allowDuplicates;
    private Function<T, String> keyExtractor;
    private Map<String, Object> map = new ConcurrentHashMap<>();

    public SavePersonProcessor(boolean allowDuplicates, Function<T, String> keyExtractor) {
        this.allowDuplicates = allowDuplicates;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public T process(T item) throws Exception {
        if(allowDuplicates) {
            return item;
        }else{
            String key = keyExtractor.apply(item);
            if(map.containsKey(key)) {
                return null;
            }
            map.put(key, item);
            return item;
        }
    }
}
