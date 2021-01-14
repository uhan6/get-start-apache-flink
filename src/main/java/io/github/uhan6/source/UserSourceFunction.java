package io.github.uhan6.source;

import io.github.uhan6.entity.UserEntity;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;

public class UserSourceFunction extends RichSourceFunction<UserEntity> {
    final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    final String URL = "jdbc:mysql://localhost:3306/";
    final String DB_NAME = "flink_source";
    final String DB_NAME_TABLE_NAME = "flink_source.user";
    final String USERNAME = "root";
    final String PASSWORD = "dev";

    Connection connect;
    PreparedStatement ps;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(MYSQL_DRIVER);
        connect = DriverManager.getConnection(URL + DB_NAME, USERNAME, PASSWORD);
        ps = connect.prepareStatement("select id, username, first_name, last_name, age, user_info, last_update_time from " + DB_NAME_TABLE_NAME);
    }

    @Override
    public void run(SourceContext<UserEntity> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            sourceContext.collect(UserEntity.builder()
                    .id(resultSet.getLong(1))
                    .username(resultSet.getString(2))
                    .firstName(resultSet.getString(3))
                    .lastName(resultSet.getString(4))
                    .age(resultSet.getInt(5))
                    .userInfo(resultSet.getString(6))
                    .lastUpdateTime(resultSet.getTimestamp(7).toLocalDateTime())
                    .build());
        }
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (Objects.nonNull(connect)) {
                connect.close();
            }
            if (Objects.nonNull(ps)) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
