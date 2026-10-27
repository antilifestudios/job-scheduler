package com.ashutosh.scheduler.worker.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Registry mapping job types to JobHandler implementations.
 * Enforces fail-fast duplicate checks on application startup.
 */
@Component
public class JobHandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(JobHandlerRegistry.class);

    private final Map<String, JobHandler> handlers;

    public JobHandlerRegistry(List<JobHandler> handlerList) {
        Map<String, JobHandler> map = new HashMap<>();
        for (JobHandler handler : handlerList) {
            String type = handler.type().trim().toUpperCase();
            if (map.containsKey(type)) {
                throw new IllegalStateException(
                    "Duplicate JobHandler registration for type '" + type + "': " +
                    map.get(type).getClass().getName() + " and " + handler.getClass().getName()
                );
            }
            map.put(type, handler);
            log.info("Registered JobHandler for type '{}' -> {}", type, handler.getClass().getSimpleName());
        }
        this.handlers = Collections.unmodifiableMap(map);
    }

    public Optional<JobHandler> getHandler(String type) {
        if (type == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(handlers.get(type.trim().toUpperCase()));
    }
}
