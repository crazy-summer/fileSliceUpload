package com.liuao.fileops;

import com.liuao.fileops.service.FileOperationService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.util.DigestUtils;

import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
class FileOpsApplicationTests {

	@Autowired
	private FileOperationService fileOperationService;

	@Test
	void contextLoads() {
	}


	@Test
	void testSlicedUpload() throws Exception {
		// 1. 准备配置
		String sourceFilePath = "C:\\Users\\sakur\\Videos\\爆裂鼓手2k高清.mp4";
		String uploadDir = "C:\\Users\\sakur\\Documents\\upload";
		String fileName = "merged_drummer.mp4";
		String fileId = UUID.randomUUID().toString();
		int totalChunks = 1000;
		int repeatTimes = 10; // 每个分片重复调用10次

		Path sourcePath = Paths.get(sourceFilePath);
		long fileSize = Files.size(sourcePath);
		long chunkSize = (long) Math.ceil((double) fileSize / totalChunks);

		// 使用固定线程池模拟高并发
		ExecutorService executor = Executors.newFixedThreadPool(100);
		CountDownLatch latch = new CountDownLatch(totalChunks * repeatTimes);

		System.out.println("开始分片上传测试，文件大小: " + (fileSize / 1024 / 1024 / 1024) + "GB");

		// 2. 模拟多线程重复上传
		for (int i = 0; i < totalChunks; i++) {
			final int chunkIndex = i;
			long offset = i * chunkSize;
			long length = Math.min(chunkSize, fileSize - offset);

			// 每个分片提交 10 次任务
			for (int r = 0; r < repeatTimes; r++) {

				executor.execute(() -> {
					try (RandomAccessFile raf = new RandomAccessFile(sourceFilePath, "r")) {
						// 读取该分片的数据
						byte[] buffer = new byte[(int) length];
						raf.seek(offset);
						raf.readFully(buffer);

						MockMultipartFile mockFile = new MockMultipartFile(
								"file",
								"chunk-" + chunkIndex,
								"application/octet-stream",
								buffer
						);
						fileOperationService.slicedUpload(mockFile, fileId, chunkIndex, totalChunks, fileName);
					} catch (Exception e) {
						log.error("分片 {} 上传任务失败", chunkIndex, e);
					} finally {
						latch.countDown();
					}
				});
			}
		}

		// 等待所有线程执行完毕
		latch.await();

		// 3. 校验逻辑
		// 注意：由于合并是 @Async 异步的，这里需要轮询等待合并完成
		Path targetPath = Paths.get(uploadDir, fileId + "_" + fileName);
		int waitTime = 0;
		while (waitTime < 120) { // 考虑到 10GB 合并可能超过 60 秒，建议放宽到 120
			if (Files.exists(targetPath)) {
				long currentSize = Files.size(targetPath);
				System.out.println("等待合并中 (进度: " + (currentSize * 100 / fileSize) + "%)...");
				if (currentSize == fileSize) {
					break;
				}
			} else {
				System.out.println("文件尚未创建...");
			}
			Thread.sleep(1000);
			waitTime++;
		}

		// 4. 断言
		assertTrue(Files.exists(targetPath), "文件应存在于上传目录下");
        assertEquals(Files.size(targetPath), fileSize, "合并后的文件大小应与原文件一致");

		 // 可选：MD5 校验（10G文件校验极慢，视性能而定）
		 String sourceMd5 = DigestUtils.md5DigestAsHex(new FileInputStream(sourceFilePath));
		 String targetMd5 = DigestUtils.md5DigestAsHex(new FileInputStream(targetPath.toFile()));
		 assertEquals(sourceMd5, targetMd5, "文件MD5校验失败");

		executor.shutdown();
	}
	//RandomAccessFile 分片读取：由于是 10GB 的超大文件，我们不能一次性读入内存。代码中使用 raf.seek() 精确跳转到每个分片的位置读取数据，既节省内存又保证了数据的准确性。
	// 并发控制 (CountDownLatch)：测试线程会等待所有分片的 $1000 \times 10$ 次请求全部发出并响应后才继续向下执行。
	// 轮询等待异步任务：由于你的 mergeChunksAsync 方法标注了 @Async，主线程调用完最后一次上传后会立即返回。我们在校验部分加了一个 while 循环轮询文件是否存在。
	// 内存安全：虽然文件是 10GB，但代码每次只读取一个分片的内容（约 10MB）放入 MockMultipartFile，防止 JVM 出现 OutOfMemoryError。
}
