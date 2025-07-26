package com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.combinedstreams;

import com.binance.connector.client.common.websocket.service.StreamBlockingQueueWrapper;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.DerivativesTradingUsdsFuturesWebSocketStreamsUtil;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.api.DerivativesTradingUsdsFuturesWebSocketStreams;

import java.util.Set;

/**
 * Example demonstrating how to use combined streams to subscribe to multiple market data streams
 * in a single WebSocket connection. The returned data is in raw String format, allowing the
 * consumer to parse and handle different stream types as needed.
 */
public class CombinedStreamsExample {
    public static void main(String[] args) {
        
        // Initialize the WebSocket client
        DerivativesTradingUsdsFuturesWebSocketStreams webSocketStreams = 
            new DerivativesTradingUsdsFuturesWebSocketStreams(
                DerivativesTradingUsdsFuturesWebSocketStreamsUtil.getClientConfiguration());

        // Define the streams you want to subscribe to
        Set<String> streamNames = Set.of(
            "btcusdt@aggTrade",        // Bitcoin aggregate trade stream
            "ethusdt@ticker",          // Ethereum 24hr ticker statistics
            "adausdt@depth@100ms"      // Cardano order book updates
        );

        try {
            // Subscribe to combined streams
            StreamBlockingQueueWrapper<String> combinedStreamQueue = 
                webSocketStreams.combinedStreams(streamNames);

            System.out.println("Successfully subscribed to combined streams:");
            streamNames.forEach(stream -> System.out.println("  - " + stream));
            System.out.println();
            System.out.println("Receiving data (first 10 messages):");

            // Read and display the first 10 messages
            for (int i = 0; i < 10; i++) {
                String message = combinedStreamQueue.take();
                System.out.println("Message " + (i + 1) + ": " + message);
            }

        } catch (Exception e) {
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}