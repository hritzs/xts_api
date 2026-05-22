package com.example.greekauthservice.dto;

import lombok.Data;

@Data // Lombok annotation to generate getters, setters, toString, etc.
public class AuthRequest {
    private String username;
    private String password;
}