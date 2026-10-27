package com.ashutosh.scheduler.security.apikey;

import com.ashutosh.scheduler.security.SecurityUser;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/keys")
public class ApiKeyController {

    private final ApiKeyService apiKeyService;

    public ApiKeyController(ApiKeyService apiKeyService) {
        this.apiKeyService = apiKeyService;
    }

    @PostMapping
    public ResponseEntity<CreateApiKeyResponse> createApiKey(
            @AuthenticationPrincipal SecurityUser currentUser,
            @RequestBody(required = false) CreateApiKeyRequest request) {
        CreateApiKeyResponse response = apiKeyService.createApiKey(currentUser.userId(), request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping
    public ResponseEntity<List<ApiKeyResponse>> listApiKeys(
            @AuthenticationPrincipal SecurityUser currentUser) {
        List<ApiKeyResponse> response = apiKeyService.listApiKeys(currentUser.userId());
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<ApiKeyResponse> revokeApiKey(
            @AuthenticationPrincipal SecurityUser currentUser,
            @PathVariable UUID id) {
        ApiKeyResponse response = apiKeyService.revokeApiKey(currentUser.userId(), id);
        return ResponseEntity.ok(response);
    }
}
