package com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.api;

import com.binance.connector.client.common.ApiException;
import com.binance.connector.client.common.SystemUtil;
import com.binance.connector.client.common.websocket.adapter.stream.StreamConnectionInterface;
import com.binance.connector.client.common.websocket.adapter.stream.StreamConnectionPoolWrapper;
import com.binance.connector.client.common.websocket.adapter.stream.StreamConnectionWrapper;
import com.binance.connector.client.common.websocket.configuration.WebSocketClientConfiguration;
import com.binance.connector.client.common.websocket.dtos.RequestWrapperDTO;
import com.binance.connector.client.common.websocket.service.StreamBlockingQueue;
import com.binance.connector.client.common.websocket.service.StreamBlockingQueueWrapper;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.JSON;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.AggregateTradeStreamsRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.AggregateTradeStreamsResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.AllBookTickersStreamRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.AllBookTickersStreamResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.AllMarketLiquidationOrderStreamsRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.AllMarketLiquidationOrderStreamsResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.AllMarketMiniTickersStreamRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.AllMarketMiniTickersStreamResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.AllMarketTickersStreamsRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.AllMarketTickersStreamsResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.CompositeIndexSymbolInformationStreamsRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.CompositeIndexSymbolInformationStreamsResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.ContinuousContractKlineCandlestickStreamsRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.ContinuousContractKlineCandlestickStreamsResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.ContractInfoStreamRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.ContractInfoStreamResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.DiffBookDepthStreamsRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.DiffBookDepthStreamsResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.IndividualSymbolBookTickerStreamsRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.IndividualSymbolBookTickerStreamsResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.IndividualSymbolMiniTickerStreamRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.IndividualSymbolMiniTickerStreamResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.IndividualSymbolTickerStreamsRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.IndividualSymbolTickerStreamsResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.KlineCandlestickStreamsRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.KlineCandlestickStreamsResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.LiquidationOrderStreamsRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.LiquidationOrderStreamsResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.MarkPriceStreamForAllMarketRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.MarkPriceStreamForAllMarketResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.MarkPriceStreamRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.MarkPriceStreamResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.MultiAssetsModeAssetIndexRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.MultiAssetsModeAssetIndexResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.PartialBookDepthStreamsRequest;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.PartialBookDepthStreamsResponse;
import com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.model.UserDataStreamEventsResponse;
import com.google.gson.reflect.TypeToken;
import java.util.Collections;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class DerivativesTradingUsdsFuturesWebSocketStreams {
    private static final String USER_AGENT =
            String.format(
                    "binance-derivatives-trading-usds-futures/4.0.0 (Java/%s; %s; %s)",
                    SystemUtil.getJavaVersion(), SystemUtil.getOs(), SystemUtil.getArch());

    private final StreamConnectionInterface connection;

    private WebsocketMarketStreamsApi websocketMarketStreamsApi;

    public DerivativesTradingUsdsFuturesWebSocketStreams(
            WebSocketClientConfiguration configuration) {
        this(
                configuration.getUsePool()
                        ? new StreamConnectionPoolWrapper(configuration, JSON.getGson())
                        : new StreamConnectionWrapper(configuration, JSON.getGson()));
    }

    public DerivativesTradingUsdsFuturesWebSocketStreams(StreamConnectionInterface connection) {
        connection.setUserAgent(USER_AGENT);
        if (!connection.isConnected()) {
            connection.connect();
        }
        this.connection = connection;

        this.websocketMarketStreamsApi = new WebsocketMarketStreamsApi(connection);
    }

    public StreamBlockingQueueWrapper<AggregateTradeStreamsResponse> aggregateTradeStreams(
            AggregateTradeStreamsRequest aggregateTradeStreamsRequest) throws ApiException {
        return websocketMarketStreamsApi.aggregateTradeStreams(aggregateTradeStreamsRequest);
    }

    public StreamBlockingQueueWrapper<AllBookTickersStreamResponse> allBookTickersStream(
            AllBookTickersStreamRequest allBookTickersStreamRequest) throws ApiException {
        return websocketMarketStreamsApi.allBookTickersStream(allBookTickersStreamRequest);
    }

    public StreamBlockingQueueWrapper<AllMarketLiquidationOrderStreamsResponse>
            allMarketLiquidationOrderStreams(
                    AllMarketLiquidationOrderStreamsRequest allMarketLiquidationOrderStreamsRequest)
                    throws ApiException {
        return websocketMarketStreamsApi.allMarketLiquidationOrderStreams(
                allMarketLiquidationOrderStreamsRequest);
    }

    public StreamBlockingQueueWrapper<AllMarketMiniTickersStreamResponse>
            allMarketMiniTickersStream(
                    AllMarketMiniTickersStreamRequest allMarketMiniTickersStreamRequest)
                    throws ApiException {
        return websocketMarketStreamsApi.allMarketMiniTickersStream(
                allMarketMiniTickersStreamRequest);
    }

    public StreamBlockingQueueWrapper<AllMarketTickersStreamsResponse> allMarketTickersStreams(
            AllMarketTickersStreamsRequest allMarketTickersStreamsRequest) throws ApiException {
        return websocketMarketStreamsApi.allMarketTickersStreams(allMarketTickersStreamsRequest);
    }

    public StreamBlockingQueueWrapper<CompositeIndexSymbolInformationStreamsResponse>
            compositeIndexSymbolInformationStreams(
                    CompositeIndexSymbolInformationStreamsRequest
                            compositeIndexSymbolInformationStreamsRequest)
                    throws ApiException {
        return websocketMarketStreamsApi.compositeIndexSymbolInformationStreams(
                compositeIndexSymbolInformationStreamsRequest);
    }

    public StreamBlockingQueueWrapper<ContinuousContractKlineCandlestickStreamsResponse>
            continuousContractKlineCandlestickStreams(
                    ContinuousContractKlineCandlestickStreamsRequest
                            continuousContractKlineCandlestickStreamsRequest)
                    throws ApiException {
        return websocketMarketStreamsApi.continuousContractKlineCandlestickStreams(
                continuousContractKlineCandlestickStreamsRequest);
    }

    public StreamBlockingQueueWrapper<ContractInfoStreamResponse> contractInfoStream(
            ContractInfoStreamRequest contractInfoStreamRequest) throws ApiException {
        return websocketMarketStreamsApi.contractInfoStream(contractInfoStreamRequest);
    }

    public StreamBlockingQueueWrapper<DiffBookDepthStreamsResponse> diffBookDepthStreams(
            DiffBookDepthStreamsRequest diffBookDepthStreamsRequest) throws ApiException {
        return websocketMarketStreamsApi.diffBookDepthStreams(diffBookDepthStreamsRequest);
    }

    public StreamBlockingQueueWrapper<IndividualSymbolBookTickerStreamsResponse>
            individualSymbolBookTickerStreams(
                    IndividualSymbolBookTickerStreamsRequest
                            individualSymbolBookTickerStreamsRequest)
                    throws ApiException {
        return websocketMarketStreamsApi.individualSymbolBookTickerStreams(
                individualSymbolBookTickerStreamsRequest);
    }

    public StreamBlockingQueueWrapper<IndividualSymbolMiniTickerStreamResponse>
            individualSymbolMiniTickerStream(
                    IndividualSymbolMiniTickerStreamRequest individualSymbolMiniTickerStreamRequest)
                    throws ApiException {
        return websocketMarketStreamsApi.individualSymbolMiniTickerStream(
                individualSymbolMiniTickerStreamRequest);
    }

    public StreamBlockingQueueWrapper<IndividualSymbolTickerStreamsResponse>
            individualSymbolTickerStreams(
                    IndividualSymbolTickerStreamsRequest individualSymbolTickerStreamsRequest)
                    throws ApiException {
        return websocketMarketStreamsApi.individualSymbolTickerStreams(
                individualSymbolTickerStreamsRequest);
    }

    public StreamBlockingQueueWrapper<KlineCandlestickStreamsResponse> klineCandlestickStreams(
            KlineCandlestickStreamsRequest klineCandlestickStreamsRequest) throws ApiException {
        return websocketMarketStreamsApi.klineCandlestickStreams(klineCandlestickStreamsRequest);
    }

    public StreamBlockingQueueWrapper<LiquidationOrderStreamsResponse> liquidationOrderStreams(
            LiquidationOrderStreamsRequest liquidationOrderStreamsRequest) throws ApiException {
        return websocketMarketStreamsApi.liquidationOrderStreams(liquidationOrderStreamsRequest);
    }

    public StreamBlockingQueueWrapper<MarkPriceStreamResponse> markPriceStream(
            MarkPriceStreamRequest markPriceStreamRequest) throws ApiException {
        return websocketMarketStreamsApi.markPriceStream(markPriceStreamRequest);
    }

    public StreamBlockingQueueWrapper<MarkPriceStreamForAllMarketResponse>
            markPriceStreamForAllMarket(
                    MarkPriceStreamForAllMarketRequest markPriceStreamForAllMarketRequest)
                    throws ApiException {
        return websocketMarketStreamsApi.markPriceStreamForAllMarket(
                markPriceStreamForAllMarketRequest);
    }

    public StreamBlockingQueueWrapper<MultiAssetsModeAssetIndexResponse> multiAssetsModeAssetIndex(
            MultiAssetsModeAssetIndexRequest multiAssetsModeAssetIndexRequest) throws ApiException {
        return websocketMarketStreamsApi.multiAssetsModeAssetIndex(
                multiAssetsModeAssetIndexRequest);
    }

    public StreamBlockingQueueWrapper<PartialBookDepthStreamsResponse> partialBookDepthStreams(
            PartialBookDepthStreamsRequest partialBookDepthStreamsRequest) throws ApiException {
        return websocketMarketStreamsApi.partialBookDepthStreams(partialBookDepthStreamsRequest);
    }

    /**
     * Subscribes to combined public market data streams.
     * 
     * @param streamNames - Collection of stream names to subscribe to (e.g., "btcusdt@aggTrade", "ethusdt@ticker")
     * @return A WebSocket stream handler for the combined streams returning raw String data
     * @throws ApiException if the stream names are null or empty
     */
    public StreamBlockingQueueWrapper<String> combinedStreams(Collection<String> streamNames) throws ApiException {
        if (streamNames == null || streamNames.isEmpty()) {
            throw new ApiException("Stream names cannot be null or empty");
        }
        
        Set<String> streamSet = new HashSet<>(streamNames);
        RequestWrapperDTO<Set<String>, Object> requestWrapperDTO =
                new RequestWrapperDTO.Builder<Set<String>, Object>()
                        .id(getRequestID())
                        .method("SUBSCRIBE")
                        .params(streamSet)
                        .build();
        
        Map<String, StreamBlockingQueue<String>> queuesMap =
                connection.subscribe(requestWrapperDTO);
        
        // Create a multiplexing queue that aggregates messages from all individual stream queues
        // This is necessary because StreamConnectionWrapper routes messages to individual queues
        // based on the "stream" field, but we want all messages in a single combined queue
        StreamBlockingQueue<String> combinedQueue = createCombinedQueue(queuesMap);
        
        // Return raw String wrapper without type conversion
        TypeToken<String> typeToken = new TypeToken<String>() {};
        return new StreamBlockingQueueWrapper<>(combinedQueue, typeToken, JSON.getGson());
    }
    
    /**
     * Creates a combined queue that multiplexes messages from individual stream queues
     * into a single output queue for combined streams functionality.
     */
    private StreamBlockingQueue<String> createCombinedQueue(Map<String, StreamBlockingQueue<String>> individualQueues) {
        // Create the output queue for combined messages
        java.util.concurrent.LinkedBlockingDeque<String> outputQueue = new java.util.concurrent.LinkedBlockingDeque<>();
        String combinedStreamId = "combined_" + getRequestID();
        StreamBlockingQueue<String> combinedQueue = new StreamBlockingQueue<>(outputQueue, combinedStreamId);
        
        // Start background threads to multiplex messages from individual queues to the combined queue
        for (Map.Entry<String, StreamBlockingQueue<String>> entry : individualQueues.entrySet()) {
            String streamName = entry.getKey();
            StreamBlockingQueue<String> individualQueue = entry.getValue();
            
            // Start a daemon thread to forward messages from individual queue to combined queue
            Thread multiplexer = new Thread(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            String message = individualQueue.take();
                            // Forward the message to the combined queue
                            combinedQueue.offer(message);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                } catch (Exception e) {
                    // Log error but don't stop the thread completely
                    System.err.println("Error in stream multiplexer for " + streamName + ": " + e.getMessage());
                }
            });
            multiplexer.setDaemon(true);
            multiplexer.setName("stream-multiplexer-" + streamName);
            multiplexer.start();
        }
        
        return combinedQueue;
    }

    /**
     * Subscribes to combined public market data streams.
     * 
     * @param streamNames - Array of stream names to subscribe to (e.g., "btcusdt@aggTrade", "ethusdt@ticker")
     * @return A WebSocket stream handler for the combined streams returning raw String data
     * @throws ApiException if the stream names are null or empty
     */
    public StreamBlockingQueueWrapper<String> combinedStreams(String... streamNames) throws ApiException {
        if (streamNames == null || streamNames.length == 0) {
            throw new ApiException("Stream names cannot be null or empty");
        }
        return combinedStreams(Set.of(streamNames));
    }

    /**
     * Subscribes to the user data WebSocket stream using the provided listen key.
     *
     * @param listenKey - The listen key for the user data WebSocket stream.
     * @return A WebSocket stream handler for the user data stream.
     */
    public StreamBlockingQueueWrapper<UserDataStreamEventsResponse> userData(String listenKey) {
        RequestWrapperDTO<Set<String>, Object> requestWrapperDTO =
                new RequestWrapperDTO.Builder<Set<String>, Object>()
                        .id(getRequestID())
                        .method("SUBSCRIBE")
                        .params(Collections.singleton(listenKey))
                        .build();
        Map<String, StreamBlockingQueue<String>> queuesMap =
                connection.subscribe(requestWrapperDTO);

        TypeToken<UserDataStreamEventsResponse> typeToken = new TypeToken<>() {};
        StreamBlockingQueue<String> queue = queuesMap.get(listenKey);
        return new StreamBlockingQueueWrapper<>(queue, typeToken, JSON.getGson());
    }

    public String getRequestID() {
        return UUID.randomUUID().toString();
    }
}
