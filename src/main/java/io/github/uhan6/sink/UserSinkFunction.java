package io.github.uhan6.sink;

import io.github.uhan6.entity.UserEntity;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Objects;

public class UserSinkFunction extends RichSinkFunction<UserEntity> {
    final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    final String URL = "jdbc:mysql://localhost:3306/";
    final String DB_NAME = "flink_sink";
    final String DB_NAME_TABLE_NAME = "flink_sink.user_sink";
    final String USERNAME = "root";
    final String PASSWORD = "dev";

    Connection connect;
    PreparedStatement ps;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(MYSQL_DRIVER);
        connect = DriverManager.getConnection(URL + DB_NAME, USERNAME, PASSWORD);
        ps = connect.prepareStatement("INSERT INTO " + DB_NAME_TABLE_NAME + " (id, username, first_name, last_name, age, user_info, last_update_time) VALUES (?,?,?,?,?,?,?)");
    }

    @Override
    public void invoke(UserEntity value, Context context) throws Exception {
        ps.setLong(1, value.getId());
        ps.setString(2, value.getUsername());
        ps.setString(3, value.getFirstName());
        ps.setString(4, value.getLastName());
        ps.setInt(5, value.getAge());
        ps.setString(6, value.getUserInfo());
        ps.setTimestamp(7, java.sql.Timestamp.valueOf(value.getLastUpdateTime()));

        ps.executeUpdate();
    }

    @Override
    public void close() {
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
