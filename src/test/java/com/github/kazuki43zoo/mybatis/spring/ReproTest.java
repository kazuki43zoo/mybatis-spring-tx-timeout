package com.github.kazuki43zoo.mybatis.spring;

import org.apache.ibatis.annotations.Select;
import org.junit.Test;
import org.mybatis.spring.MyBatisSystemException;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ReproTest {

    @Test
    public void mybatisBeforeTimeout() throws InterruptedException {
        try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(MyBatisDataSourceContext.class)) {
            TransactionalService service = ctx.getBean(TransactionalService.class);
            service.execute("mybatisBeforeTimeout", 0, 0);
        }
    }

    @Test(expected = MyBatisSystemException.class)
    public void mybatisAfterTimeout() throws InterruptedException {
        try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(MyBatisDataSourceContext.class)) {
            TransactionalService service = ctx.getBean(TransactionalService.class);
            service.execute("mybatisAfterTimeout", 3, 0);
        }
    }

    @Test
    public void jdbcTemplateBeforeTimeout() throws InterruptedException {
        try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(JdbcTemplateDataSourceContext.class)) {
            TransactionalService service = ctx.getBean(TransactionalService.class);
            service.execute("jdbcTemplateBeforeTimeout", 0, 0);
        }
    }

    @Test(expected = TransactionException.class)
    public void jdbcTemplateAfterTimeout() throws InterruptedException {
        try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(JdbcTemplateDataSourceContext.class)) {
            TransactionalService service = ctx.getBean(TransactionalService.class);
            service.execute("jdbcTemplateAfterTimeout", 3, 0);
        }
    }


    @EnableTransactionManagement
    @Configuration
    @MapperScan("com.github.kazuki43zoo.mybatis.spring")
    static class MyBatisDataSourceContext {

        @Bean
        DataSource dataSource() {
            return new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2).build();
        }

        @Bean
        DataSourceTransactionManager transactionManager() {
            return new DataSourceTransactionManager(dataSource());
        }

        @Bean
        SqlSessionFactoryBean sqlSessionFactory(DataSource dataSource) {
            SqlSessionFactoryBean factoryBean = new SqlSessionFactoryBean();
            factoryBean.setDataSource(dataSource);
            return factoryBean;
        }

        @Bean
        TransactionalService service(Mapper mapper) {
            return new TransactionalService(() -> mapper.findCurrentTimestamp());
        }

    }

    @EnableTransactionManagement
    @Configuration
    static class JdbcTemplateDataSourceContext {

        @Bean
        DataSource dataSource() {
            return new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2).build();
        }

        @Bean
        DataSourceTransactionManager transactionManager() {
            return new DataSourceTransactionManager(dataSource());
        }

        @Bean
        JdbcTemplate jdbcTemplate() {
            return new JdbcTemplate(dataSource());
        }

        @Bean
        TransactionalService service(JdbcTemplate jdbcTemplate) {
            return new TransactionalService(() -> jdbcTemplate.queryForObject("SELECT CURRENT_TIMESTAMP()", Timestamp.class));
        }

    }

    @Transactional(timeout = 2) // 2 sec
    @Service
    static class TransactionalService {

        private final Supplier<Timestamp> timestampSupplier;

        public TransactionalService(Supplier<Timestamp> timestampSupplier) {
            this.timestampSupplier = timestampSupplier;
        }

        public void execute(String msg, int beforeWaitSec, int afterWaitSec) throws InterruptedException {
            TimeUnit.SECONDS.sleep(beforeWaitSec);
            System.out.println(timestampSupplier.get() + " : " + msg);
            TimeUnit.SECONDS.sleep(afterWaitSec);
        }

    }

    interface Mapper {
        @Select("SELECT CURRENT_TIMESTAMP()")
        Timestamp findCurrentTimestamp();
    }

}