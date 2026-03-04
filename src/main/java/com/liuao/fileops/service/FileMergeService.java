package com.liuao.fileops.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;

@Slf4j
@Service
public class FileMergeService {
    @Autowired
    RedisTemplate<String, Object> redisTemplate;

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
