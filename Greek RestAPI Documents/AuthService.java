package com.example.greekauthservice.service;

import com.example.greekauthservice.dto.ExternalTokenRequest;
import com.example.greekauthservice.dto.ExternalTokenResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
public class AuthService {

    private final WebClient webClient;

    @Value("${greek.api.token-url}")
    private String tokenUrl;

    public AuthService(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }

    public Mono<String> getSessionToken(String username, String password) {
        ExternalTokenRequest requestBody = new ExternalTokenRequest(username, password);

        return webClient.post()
                .uri(tokenUrl)
                .bodyValue(requestBody)
                .retrieve()
                .onStatus(httpStatus -> httpStatus.value() != 201,
                        clientResponse -> Mono.error(new RuntimeException("Failed to authenticate with external API. Status: " + clientResponse.statusCode())))
                .bodyToMono(ExternalTokenResponse.class)
                .map(ExternalTokenResponse::getSessionToken);
    }
}