package com.liuao.fileops;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.RedisCallback;

@Slf4j
@SpringBootTest
public class RedisConnectionTests {
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Test
    void testRedisConnection() {
        try {
            // 1. 基础读写测试
            stringRedisTemplate.opsForValue().set("test_key", "hello_gemini");
            String value = stringRedisTemplate.opsForValue().get("test_key");
            System.out.println("测试数据读取: " + value);

            // 2. 使用 StringRedisTemplate 执行 Ping，避开泛型推断问题
            String pong = stringRedisTemplate.execute((RedisCallback<String>) connection ->
                    connection.ping() != null ? "PONG" : "FAIL"
            );

            System.out.println("Redis 连接成功，Ping 回应: " + pong);

            assert "hello_gemini".equals(value);
            assert "PONG".equalsIgnoreCase(pong);

        } catch (Exception e) {
            System.err.println("无法连接到 Redis: " + e.getMessage());
            throw e;
        }
    }
}
