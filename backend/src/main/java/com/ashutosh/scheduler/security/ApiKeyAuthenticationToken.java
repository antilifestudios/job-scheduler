package com.ashutosh.scheduler.security;

import org.springframework.security.authentication.AbstractAuthenticationToken;

public class ApiKeyAuthenticationToken extends AbstractAuthenticationToken {

    private final SecurityUser principal;

    public ApiKeyAuthenticationToken(SecurityUser principal) {
        super(principal.getAuthorities());
        this.principal = principal;
        setAuthenticated(true);
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public SecurityUser getPrincipal() {
        return principal;
    }
}
