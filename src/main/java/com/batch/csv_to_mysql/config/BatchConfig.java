package com.batch.csv_to_mysql.config;

import com.batch.csv_to_mysql.entities.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;

import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private DataSource dataSource;
//    @Autowired
//    private JobBuilderFactory jobBuilderFactory;
//
//    @Autowired
//    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;


    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

//    public BatchConfig(DataSource dataSource, JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
//        this.dataSource = dataSource;
//        this.jobBuilderFactory = jobBuilderFactory;
//        this.stepBuilderFactory = stepBuilderFactory;
//    }


    @Bean
    public JobRepository jobRepository(){
        JobRepositoryFactoryBean jReposFact = new JobRepositoryFactoryBean();
        jReposFact.setDataSource(this.dataSource);
        jReposFact.setTransactionManager(new DataSourceTransactionManager());
        jReposFact.setIsolationLevelForCreate("ISOLATION_REPEATABLE_READ");

       return jobRepository;
    }


    @Bean
    public JdbcBatchItemWriter<User> writer() {
        JdbcBatchItemWriter<User> writer = new JdbcBatchItemWriter<>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.setSql("INSERT INTO user(userId, firstName, lastName, city) VALUES (:userId, :firstName, :lastName, :city)");
        writer.setDataSource(this.dataSource);
        return writer;
    }



    @Bean
    public FlatFileItemReader<User> reader() {
        FlatFileItemReader<User> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("example.csv"));
        reader.setLineMapper(getLineMapper());
        reader.setLinesToSkip(1);
        return reader;
    }

    private LineMapper<User> getLineMapper() {
        DefaultLineMapper<User> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();

        tokenizer.setNames("Customer Id", "First Name", "Last Name", "City");
        tokenizer.setIncludedFields(1, 2, 3, 5);

        BeanWrapperFieldSetMapper<User> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(User.class);

        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    @Bean
    public UserItemProcessor processor() {
        return new UserItemProcessor();
    }



    @Bean
    public Tasklet user() {
        return (Tasklet) new UserItemProcessor();
    }

    @Bean
    public Job importUserJob() {
        return new JobBuilder("USER-IMPORT-JOB",jobRepository)
                .incrementer(new RunIdIncrementer())
                .flow(step1())
                .end()
                .build();
    }



    @Bean
    public Step step1() {
        return new StepBuilder("step1",jobRepository)
                .<User, User>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }
//
//    @Bean
//    public Step step1(JobRepository jobRepository, Tasklet myTasklet, PlatformTransactionManager transactionManager) {
//        return new StepBuilder("step1", jobRepository)
//                .tasklet(myTasklet, transactionManager) // or .chunk(chunkSize, transactionManager)
//                .build();
//    }

    @Bean
    public StepBuilderFactory stepBuilderFactory(){
        return  new StepBuilderFactory(jobRepository);
    }

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new DataSourceTransactionManager(dataSource);
    }

}


