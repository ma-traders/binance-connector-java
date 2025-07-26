package com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.combinedstreams;

import com.binance.connector.client.common.websocket.service.StreamBlockingQueueWrapper;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.DerivativesTradingUsdsFuturesWebSocketStreamsUtil;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.api.DerivativesTradingUsdsFuturesWebSocketStreams;

/**
 * Example demonstrating how to use combined streams with varargs parameter to subscribe 
 * to multiple market data streams in a single WebSocket connection.
 */
public class CombinedStreamsVarargsExample {
    public static void main(String[] args) {
        
        // Initialize the WebSocket client
        DerivativesTradingUsdsFuturesWebSocketStreams webSocketStreams = 
            new DerivativesTradingUsdsFuturesWebSocketStreams(
                DerivativesTradingUsdsFuturesWebSocketStreamsUtil.getClientConfiguration());

        try {
            // Subscribe to combined streams using varargs
            StreamBlockingQueueWrapper<String> combinedStreamQueue = 
                webSocketStreams.combinedStreams(
                    "btcusdt@aggTrade",
                    "ethusdt@ticker", 
                    "adausdt@depth@100ms",
                    "bnbusdt@kline_1m"
                );

            System.out.println("Successfully subscribed to combined streams using varargs");
            System.out.println("Streams: btcusdt@aggTrade, ethusdt@ticker, adausdt@depth@100ms, bnbusdt@kline_1m");
            System.out.println();
            System.out.println("Receiving data (first 5 messages):");

            // Read and display the first 5 messages
            for (int i = 0; i < 5; i++) {
                String message = combinedStreamQueue.take();
                System.out.println("Message " + (i + 1) + ": " + message);
            }

        } catch (Exception e) {
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}