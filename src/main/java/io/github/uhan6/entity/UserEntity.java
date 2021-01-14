package io.github.uhan6.entity;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Builder
public class UserEntity implements Serializable {
    private Long id;
    private String username;
    private String firstName;
    private String lastName;
    private Integer age;
    private String userInfo;
    private LocalDateTime lastUpdateTime;
}
