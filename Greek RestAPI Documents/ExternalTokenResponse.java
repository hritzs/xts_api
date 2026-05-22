package com.example.greekauthservice.dto;

import lombok.Data;

@Data
public class ExternalTokenResponse {
    private int id;
    private String sessionToken;
}