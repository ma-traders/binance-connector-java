package com.binance.connector.client.derivatives_trading_usds_futures.websocket.stream.api;

import com.binance.connector.client.common.ApiException;
import com.binance.connector.client.common.configuration.SignatureConfiguration;
import com.binance.connector.client.common.websocket.adapter.stream.StreamConnectionWrapper;
import com.binance.connector.client.common.websocket.configuration.WebSocketClientConfiguration;
import com.binance.connector.client.common.websocket.dtos.RequestWrapperDTO;
import com.binance.connector.client.common.websocket.service.StreamBlockingQueueWrapper;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

/** API tests for DerivativesTradingUsdsFuturesWebSocketStreams */
public class DerivativesTradingUsdsFuturesWebSocketStreamsTest {

    private DerivativesTradingUsdsFuturesWebSocketStreams streams;
    private StreamConnectionWrapper connectionSpy;
    private Session sessionMock;

    @BeforeEach
    public void initApiClient() throws Exception {
        URL resource = DerivativesTradingUsdsFuturesWebSocketStreams.class.getResource("/test-ed25519-prv-key.pem");
        SignatureConfiguration signatureConfiguration = new SignatureConfiguration();
        signatureConfiguration.setApiKey("apiKey");
        File file = new File(resource.toURI());
        signatureConfiguration.setPrivateKey(file.getAbsolutePath());
        WebSocketClientConfiguration clientConfiguration = new WebSocketClientConfiguration();
        clientConfiguration.setAutoLogon(false);
        clientConfiguration.setSignatureConfiguration(signatureConfiguration);
        clientConfiguration.setUrl("wss://localhost:8080");

        WebSocketClient webSocketClient = Mockito.mock(WebSocketClient.class);
        HttpClient httpClient = Mockito.mock(HttpClient.class);
        Mockito.doReturn(httpClient).when(webSocketClient).getHttpClient();
        
        CompletableFuture<Session> sessionCompletableFuture = new CompletableFuture<>();
        Mockito.doReturn(sessionCompletableFuture)
                .when(webSocketClient)
                .connect(Mockito.any(), Mockito.any(), Mockito.any());
        sessionMock = Mockito.mock(Session.class);

        RemoteEndpoint remoteEndpointMock = Mockito.mock(RemoteEndpoint.class);
        Mockito.doReturn(remoteEndpointMock).when(sessionMock).getRemote();

        sessionCompletableFuture.complete(sessionMock);
        StreamConnectionWrapper connectionWrapper =
                new StreamConnectionWrapper(clientConfiguration, webSocketClient);
        connectionSpy = Mockito.spy(connectionWrapper);
        Mockito.doReturn(1736393892000L).when(connectionSpy).getTimestamp();
        connectionSpy.connect();
        
        DerivativesTradingUsdsFuturesWebSocketStreams streamsObj = 
            new DerivativesTradingUsdsFuturesWebSocketStreams(connectionSpy);
        streams = Mockito.spy(streamsObj);
        Mockito.doReturn("eaf3292c-64b6-4c04-ad4f-4ca2608b42b4").when(streams).getRequestID();
    }

    /**
     * Test combined streams with Collection parameter
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void combinedStreamsCollectionTest() throws ApiException {
        Set<String> streamNames = Set.of("btcusdt@aggTrade", "ethusdt@ticker");
        
        StreamBlockingQueueWrapper<String> response = streams.combinedStreams(streamNames);
        
        ArgumentCaptor<RequestWrapperDTO<Set<String>, Object>> callArgumentCaptor = 
            ArgumentCaptor.forClass(RequestWrapperDTO.class);
        Mockito.verify(connectionSpy).subscribe(callArgumentCaptor.capture());
        
        RequestWrapperDTO<Set<String>, Object> requestWrapperDTO = callArgumentCaptor.getValue();
        Set<String> params = requestWrapperDTO.getParams();
        
        assertEquals("SUBSCRIBE", requestWrapperDTO.getMethod());
        assertEquals("eaf3292c-64b6-4c04-ad4f-4ca2608b42b4", requestWrapperDTO.getId());
        assertEquals(2, params.size());
        assertTrue(params.contains("btcusdt@aggTrade"));
        assertTrue(params.contains("ethusdt@ticker"));
        assertNotNull(response);
    }

    /**
     * Test combined streams with varargs parameter
     *
     * @throws ApiException if the Api call fails
     */
    @Test
    public void combinedStreamsVarargsTest() throws ApiException {
        StreamBlockingQueueWrapper<String> response = 
            streams.combinedStreams("btcusdt@aggTrade", "ethusdt@ticker", "adausdt@depth");
        
        ArgumentCaptor<RequestWrapperDTO<Set<String>, Object>> callArgumentCaptor = 
            ArgumentCaptor.forClass(RequestWrapperDTO.class);
        Mockito.verify(connectionSpy).subscribe(callArgumentCaptor.capture());
        
        RequestWrapperDTO<Set<String>, Object> requestWrapperDTO = callArgumentCaptor.getValue();
        Set<String> params = requestWrapperDTO.getParams();
        
        assertEquals("SUBSCRIBE", requestWrapperDTO.getMethod());
        assertEquals("eaf3292c-64b6-4c04-ad4f-4ca2608b42b4", requestWrapperDTO.getId());
        assertEquals(3, params.size());
        assertTrue(params.contains("btcusdt@aggTrade"));
        assertTrue(params.contains("ethusdt@ticker"));
        assertTrue(params.contains("adausdt@depth"));
        assertNotNull(response);
    }

    /**
     * Test combined streams with null collection throws exception
     */
    @Test
    public void combinedStreamsNullCollectionTest() {
        ApiException exception = assertThrows(ApiException.class, () -> {
            streams.combinedStreams((Set<String>) null);
        });
        assertTrue(exception.getMessage().contains("Stream names cannot be null or empty"));
    }

    /**
     * Test combined streams with empty collection throws exception
     */
    @Test
    public void combinedStreamsEmptyCollectionTest() {
        ApiException exception = assertThrows(ApiException.class, () -> {
            streams.combinedStreams(Set.of());
        });
        assertTrue(exception.getMessage().contains("Stream names cannot be null or empty"));
    }

    /**
     * Test combined streams with null varargs throws exception
     */
    @Test
    public void combinedStreamsNullVarargsTest() {
        ApiException exception = assertThrows(ApiException.class, () -> {
            streams.combinedStreams((String[]) null);
        });
        assertTrue(exception.getMessage().contains("Stream names cannot be null or empty"));
    }

    /**
     * Test combined streams with empty varargs throws exception
     */
    @Test
    public void combinedStreamsEmptyVarargsTest() {
        ApiException exception = assertThrows(ApiException.class, () -> {
            streams.combinedStreams();
        });
        assertTrue(exception.getMessage().contains("Stream names cannot be null or empty"));
    }
}