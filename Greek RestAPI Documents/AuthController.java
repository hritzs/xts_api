package com.example.greekauthservice.controller;

import com.example.greekauthservice.dto.AuthRequest;
import com.example.greekauthservice.service.AuthService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @PostMapping("/login")
    public Mono<ResponseEntity<Map<String, String>>> login(@RequestBody AuthRequest authRequest) {
        return authService.getSessionToken(authRequest.getUsername(), authRequest.getPassword())
                .map(token -> ResponseEntity.ok(Map.of("sessionToken", token)))
                .onErrorResume(e -> {
                    System.err.println("Authentication error: " + e.getMessage());
                    return Mono.just(ResponseEntity.status(401).body(Map.of("error", "Invalid credentials")));
                });
    }
}