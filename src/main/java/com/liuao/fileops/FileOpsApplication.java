package com.liuao.fileops;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class FileOpsApplication {

	public static void main(String[] args) {
		SpringApplication.run(FileOpsApplication.class, args);
	}

}
