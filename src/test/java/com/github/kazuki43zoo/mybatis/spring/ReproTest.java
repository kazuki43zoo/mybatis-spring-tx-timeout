package com.github.kazuki43zoo.mybatis.spring;

import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.Test;
import org.mybatis.spring.MyBatisExceptionTranslator;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;
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

    @Test(expected = TransactionException.class)
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
            factoryBean.setPlugins(new Interceptor[]{transactionTimeoutReflector()});
            return factoryBean;
        }

        @Bean
        SqlSessionTemplate sqlSessionTemplate(SqlSessionFactory factory) {
            return new SqlSessionTemplate(factory,
                    factory.getConfiguration().getDefaultExecutorType(),
                    new TransactionalMyBatisExceptionTranslator(dataSource()));
        }

        @Bean
        TransactionTimeoutReflector transactionTimeoutReflector() {
            return new TransactionTimeoutReflector();
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

    @Intercepts({@Signature(type = StatementHandler.class, method = "prepare", args = Connection.class)})
    static class TransactionTimeoutReflector implements Interceptor {

        @Autowired
        DataSource dataSource;

        @Override
        public Object intercept(Invocation invocation) throws Throwable {
            Statement statement = (Statement) invocation.proceed();
            ConnectionHolder holder =
                    (ConnectionHolder) TransactionSynchronizationManager.getResource(dataSource);
            if (holder != null && holder.hasTimeout()) {
                // ↓ 既にタイムアウトになっている場合は、このタイミングでTransactionTimedOutExceptionが発生する。
                int transactionTimeToLiveSec = holder.getTimeToLiveInSeconds();
                int currentQueryTimeoutSec = statement.getQueryTimeout();
                // ↓ クエリタイムアウトにトランザクションタイムアウトを反映
                if (currentQueryTimeoutSec == 0 || transactionTimeToLiveSec < currentQueryTimeoutSec) {
                    statement.setQueryTimeout(transactionTimeToLiveSec);
                }
            }
            return statement;
        }

        @Override
        public Object plugin(Object target) {
            return Plugin.wrap(target, this);
        }

        @Override
        public void setProperties(Properties properties) {
        }
    }

    static class TransactionalMyBatisExceptionTranslator extends MyBatisExceptionTranslator {

        public TransactionalMyBatisExceptionTranslator(DataSource dataSource) {
            super(dataSource, true);
        }

        @Override
        public DataAccessException translateExceptionIfPossible(RuntimeException e) {
            if (e instanceof PersistenceException) {
                if (e.getCause() instanceof PersistenceException) {
                    e = (PersistenceException) e.getCause();
                }
                // ↓ SpringのTransactionExceptionはそのままスロー
                if (e.getCause() instanceof TransactionException) {
                    throw (TransactionException) e.getCause();
                }
            }
            return super.translateExceptionIfPossible(e);
        }

    }


}