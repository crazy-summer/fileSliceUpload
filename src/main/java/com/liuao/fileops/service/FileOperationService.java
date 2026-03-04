package com.liuao.fileops.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

@Slf4j
@Service
@EnableAsync
public class FileOperationService {
    @Autowired
    RedisTemplate<String, Object> redisTemplate;

    // 1. 定义 Lua 脚本 (静态常量)
    private static final String RELEASE_LOCK_LUA =
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

    public void slicedUpload(
            MultipartFile file, String fileId, Integer chunkIndex,
            Integer totalChunks, String fileName) {
        // 检查chunkIndex是否已经上传，如果已经上传，那么跳到下一步，如果没有上传，那么上传
        String redisKey = "slicedUpload:" + fileId;
        Boolean isUploaded = redisTemplate.opsForSet().isMember(redisKey, chunkIndex.toString());
        if (Boolean.FALSE.equals(isUploaded)) {
            // 2. 尝试加锁 (SETNX + TTL)
            // 即使单机，这也起到了“排队写入磁盘”的作用，requestUuid用来防止锁过期以后任务才完成，误删其他线程拿到的锁
            String lockKey = "lock:chunk:" + fileId + ":" + chunkIndex;
            String requestUuid = UUID.randomUUID().toString(); // 锁的唯一标记
            Boolean locked = redisTemplate.opsForValue().setIfAbsent(lockKey, requestUuid, Duration.ofMinutes(5));

            if (Boolean.TRUE.equals(locked)) {
                try {
                    // 2. 执行上传逻辑...
                    saveToLocalStorage(file, fileId, chunkIndex);
                }finally {
                    // 上传成功后，将索引记录到 Redis Set 中
                    redisTemplate.opsForSet().add(redisKey, chunkIndex.toString());
                    // 可选：设置过期时间（防止脏数据堆积）
                    redisTemplate.expire(redisKey, Duration.ofDays(7));

                    // 原子释放锁 (Lua 脚本)
                    // 解决“A 删了 B 的锁”的问题
                    redisTemplate.execute(
                            new DefaultRedisScript<>(RELEASE_LOCK_LUA, Long.class),
                            Collections.singletonList(lockKey),
                            requestUuid
                    );
                }
            }else {
                // 这里的逻辑可以改为：告诉前端该分片已在处理中，稍后刷新即可
                log.info("分片 {} 正在由其他线程处理", chunkIndex);
            }
        }else {
            log.info("分片:{}已经上传", chunkIndex);
        }

        // 检查分片数量是否==分片总数量，如果相等，那么异步合并文件
        Long uploadedCount = redisTemplate.opsForSet().size(redisKey);
        if (uploadedCount != null && uploadedCount.intValue() == totalChunks) {
            // 异步调用合并方法
            log.info("所有分片上传完成，开始合并文件: {}", fileId);
            mergeChunksAsync(fileId, totalChunks, fileName);
        }
    }
    // 如果一个线程拿到锁上传分片但是失败了，应该删除锁，也不会设置chunkIndex到"slicedUpload:xxx",
    // 下一个线程拿到锁，判断chunkIndex不存在，则会执行上传，上传成功就会添加chunkIndex，并且上传会覆盖上一个线程没有上传完整的chunk
    private void saveToLocalStorage(MultipartFile file, String fileId, Integer chunkIndex) {
        // 1. 定义硬编码的基础路径
        String basePath = "C:\\Users\\sakur\\Documents\\upload";

        try {
            // 2. 构造文件夹路径：basePath/fileId
            Path folderPath = Paths.get(basePath, fileId);

            // 3. 如果文件夹不存在，则创建（包括父级目录）
            if (Files.notExists(folderPath)) {
                Files.createDirectories(folderPath);
            }

            // 4. 构造分片文件路径：basePath/fileId/chunkIndex
            // 注意：建议给分片文件名加个前缀或后缀，防止系统隐藏文件冲突
            Path chunkPath = folderPath.resolve(chunkIndex.toString());

            // 5. 保存文件（如果已存在则覆盖，适合断点续传重试场景）
            Files.copy(file.getInputStream(), chunkPath, StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException e) {
            // 建议在生产环境中抛出自定义异常，方便全局异常处理器捕获
            throw new RuntimeException("存储分片失败: " + chunkIndex, e);
        }
    }

    @Async
    public void mergeChunksAsync(String fileId, Integer totalChunks, String fileName) {
        // 設置一個 10 分鐘的合併鎖
        String mergeLockKey = "lock:merge:" + fileId;
        Boolean canMerge = redisTemplate.opsForValue().setIfAbsent(mergeLockKey, "1", Duration.ofMinutes(10));
        if (Boolean.FALSE.equals(canMerge)) {
            log.info("已经有其他线程正在合并");
            return; // 已經有線程在合併了
        }

        try {
            // 執行合併邏輯...
            String basePath = "C:\\Users\\sakur\\Documents\\upload";
            Path folderPath = Paths.get(basePath, fileId);
            // 最终生成的文件路径（可以放到另一个目录）
            Path targetFilePath = Paths.get(basePath, fileId + "_" + fileName);

            try (FileChannel targetChannel = FileChannel.open(targetFilePath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

                // 严格按照 chunkIndex 顺序合并，保证文件内容正确
                for (int i = 0; i < totalChunks; i++) {
                    Path chunkPath = folderPath.resolve(String.valueOf(i));

                    // 使用零拷贝(transferTo)提高合并效率
                    try (FileChannel chunkChannel = FileChannel.open(chunkPath, StandardOpenOption.READ)) {
                        chunkChannel.transferTo(0, chunkChannel.size(), targetChannel);
                    }

                    // 合并完一个分片就删除一个，节省空间
                    Files.delete(chunkPath);
                }

                // 4. 合并完成后清理
                Files.deleteIfExists(folderPath); // 删除文件夹
                redisTemplate.delete("slicedUpload:" + fileId); // 清理 Redis 记录
                log.info("文件 {} 合并成功！路径: {}", fileName, targetFilePath);

            } catch (IOException e) {
                log.error("文件合并失败: {}", fileId, e);
                // 生产环境建议此处通知前端或记录失败状态
            }
        } catch (Exception e) {
            // 記錄失敗狀態到 Redis，供前端輪詢
            redisTemplate.opsForValue().set("fileError:" + fileId, e.getMessage(), Duration.ofHours(1));
        } finally {
            redisTemplate.delete(mergeLockKey);
        }


    }
}
