package com.zhiyueinfo.springbatch.csvtomysql.writer;

import com.zhiyueinfo.springbatch.csvtomysql.model.User;
import java.util.List;
 
import org.springframework.batch.item.ItemWriter;
 
public class ConsoleItemWriter<T> implements ItemWriter<T> {
    @Override
    public void write(List<? extends T> items) throws Exception {
        for (T item : items) {
            User itemnew = (User) item;
            System.out.println(itemnew.getName());
        }
    }
}