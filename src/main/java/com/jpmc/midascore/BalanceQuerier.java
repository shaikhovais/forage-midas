package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Balance;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class BalanceQuerier {

    private final RestTemplate restTemplate;
    private static final String BALANCE_API_URL = "http://localhost:33400/balance";

    public BalanceQuerier(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public Balance query(Long userId) {
        String url = BALANCE_API_URL + "?userId=" + userId;
        return restTemplate.getForObject(url, Balance.class);
    }
}