package com.zhiyueinfo.springbatch.csvtomysql;

import com.zhiyueinfo.springbatch.csvtomysql.model.User;
import com.zhiyueinfo.springbatch.csvtomysql.processor.UserItemProcessor;
import com.zhiyueinfo.springbatch.csvtomysql.writer.ConsoleItemWriter;
import javax.sql.DataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;


@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

  @Autowired
  public JobBuilderFactory jobBuilderFactory;

  @Autowired
  public StepBuilderFactory stepBuilderFactory;

  @Autowired
  public DataSource dataSource;

  @Value("classpath:users*.csv")
  private Resource[] inputResources;

  @Bean
  public DataSource dataSource() {
    final DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName("com.mysql.jdbc.Driver");
    dataSource.setUrl("jdbc:mysql://localhost:3307/springbatch");
    dataSource.setUsername("root");
    dataSource.setPassword("123456");

    return dataSource;
  }

  @Bean
  public FlatFileItemReader<User> reader() {
    FlatFileItemReader<User> reader = new FlatFileItemReader<User>();
    reader.setLineMapper(new DefaultLineMapper<User>() {{
      setLineTokenizer(new DelimitedLineTokenizer() {{
        setNames(new String[]{"name", "gender"});
      }});
      setFieldSetMapper(new BeanWrapperFieldSetMapper<User>() {{
        setTargetType(User.class);
      }});

    }});

    return reader;
  }

  @Bean
  public MultiResourceItemReader<User> multiResourceItemReader() {
    MultiResourceItemReader<User> resourceItemReader = new MultiResourceItemReader<User>();
    resourceItemReader.setResources(inputResources);
    resourceItemReader.setDelegate(reader());
    return resourceItemReader;
  }

  @Bean
  public UserItemProcessor processor() {
    return new UserItemProcessor();
  }

  @Bean
  public JdbcBatchItemWriter<User> writer() {
    JdbcBatchItemWriter<User> writer = new JdbcBatchItemWriter<User>();
    writer
        .setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<User>());
    writer.setSql("INSERT INTO user(name,gender) VALUES (:name,:gender)");
    writer.setDataSource(dataSource);

    return writer;
  }

  @Bean
  public Step step1() {
    return stepBuilderFactory.get("step1").<User, User>chunk(3)
        .reader(multiResourceItemReader())
        .faultTolerant()
        .skipLimit(1)
        .skip(FlatFileParseException.class)
        .processor(processor())
        .writer(customWriter())
        .build();
  }

  @Bean
  public Job importUserJob() {
    return jobBuilderFactory.get("importUserJob")
        .incrementer(new RunIdIncrementer())
        .flow(step1())
        .end()
        .build();
  }


  @Bean
  public ConsoleItemWriter<User> customWriter() {
    return new ConsoleItemWriter<User>();
  }

}