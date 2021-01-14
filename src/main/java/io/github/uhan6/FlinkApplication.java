package io.github.uhan6;

import io.github.uhan6.entity.UserEntity;
import io.github.uhan6.sink.UserSinkFunction;
import io.github.uhan6.source.UserSourceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<UserEntity> user = env.addSource(new UserSourceFunction()).name("flink_source.user");
        SingleOutputStreamOperator<UserEntity> filterUser = user.filter(userEntity -> userEntity.getAge() >= 18).name("filterUserAgeLagerThan18");
        filterUser.addSink(new UserSinkFunction()).name("flink_sink.user_sink");

        env.execute("FlinkApplicationUserToUserSink");
    }
}
