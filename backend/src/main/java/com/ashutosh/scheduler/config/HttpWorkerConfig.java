package com.ashutosh.scheduler.config;

import com.ashutosh.scheduler.worker.ssrf.SsrfAwareDns;
import com.ashutosh.scheduler.worker.ssrf.SsrfNetworkInterceptor;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.Proxy;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
public class HttpWorkerConfig {

    @Bean
    public OkHttpClient okHttpClient(SsrfAwareDns ssrfAwareDns, SsrfNetworkInterceptor ssrfNetworkInterceptor) {
        return new OkHttpClient.Builder()
            .dns(ssrfAwareDns)
            .addNetworkInterceptor(ssrfNetworkInterceptor)
            .followRedirects(false)
            .followSslRedirects(false)
            .proxy(Proxy.NO_PROXY)
            .connectTimeout(Duration.ofSeconds(5))
            .readTimeout(Duration.ofSeconds(10))
            .writeTimeout(Duration.ofSeconds(10))
            .connectionPool(new ConnectionPool(50, 5, TimeUnit.MINUTES))
            .build();
    }
}
