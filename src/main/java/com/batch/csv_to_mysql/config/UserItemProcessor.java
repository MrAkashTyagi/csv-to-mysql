package com.batch.csv_to_mysql.config;

import com.batch.csv_to_mysql.entities.User;
import org.springframework.batch.item.ItemProcessor;

public class UserItemProcessor implements ItemProcessor<User,User> {

    @Override
    public User process(User user) throws Exception {
        return user;
    }
}

