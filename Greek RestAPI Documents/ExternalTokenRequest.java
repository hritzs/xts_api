package com.example.greekauthservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ExternalTokenRequest {
    private String username;
    private String password;
    private String validFor = "30d";
}