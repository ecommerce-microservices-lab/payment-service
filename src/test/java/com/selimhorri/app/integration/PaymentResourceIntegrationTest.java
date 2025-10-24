package com.selimhorri.app.integration;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.selimhorri.app.constant.AppConstant;
import com.selimhorri.app.dto.OrderDto;
import com.selimhorri.app.dto.PaymentDto;
import com.selimhorri.app.exception.wrapper.PaymentNotFoundException;
import com.selimhorri.app.service.PaymentService;

@Tag("integration")
@SpringBootTest
@AutoConfigureMockMvc
class PaymentResourceIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private PaymentService paymentService;

    @MockBean
    private RestTemplate restTemplate;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    void shouldFetchAllPayments() throws Exception {
        // Mock data
        OrderDto orderDto = OrderDto.builder()
                .orderId(1)
                .orderStatus("IN_PAYMENT")
                .orderDate(LocalDateTime.now())
                .build();

        PaymentDto paymentDto1 = PaymentDto.builder()
                .paymentId(1)
                .paymentStatus(com.selimhorri.app.domain.PaymentStatus.IN_PROGRESS)
                .orderDto(orderDto)
                .build();

        PaymentDto paymentDto2 = PaymentDto.builder()
                .paymentId(2)
                .paymentStatus(com.selimhorri.app.domain.PaymentStatus.NOT_STARTED)
                .orderDto(orderDto)
                .build();

        List<PaymentDto> paymentDtos = List.of(paymentDto1, paymentDto2);

        // Mock service call
        when(paymentService.findAll()).thenReturn(paymentDtos);

        // Perform request and verify
        mockMvc.perform(get("/api/payments")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.collection.length()").value(2))
                .andExpect(jsonPath("$.collection[0].paymentId").value(1))
                .andExpect(jsonPath("$.collection[0].paymentStatus").value("IN_PROGRESS"))
                .andExpect(jsonPath("$.collection[1].paymentId").value(2))
                .andExpect(jsonPath("$.collection[1].paymentStatus").value("NOT_STARTED"));

        verify(paymentService, times(1)).findAll();
    }

    @Test
    void shouldFetchPaymentById() throws Exception {
        // Mock data
        OrderDto orderDto = OrderDto.builder()
                .orderId(1)
                .orderStatus("IN_PAYMENT")
                .orderDate(LocalDateTime.now())
                .build();

        PaymentDto paymentDto = PaymentDto.builder()
                .paymentId(1)
                .paymentStatus(com.selimhorri.app.domain.PaymentStatus.IN_PROGRESS)
                .orderDto(orderDto)
                .build();

        // Mock service call
        when(paymentService.findById(anyInt())).thenReturn(paymentDto);

        // Perform request and verify
        mockMvc.perform(get("/api/payments/{paymentId}", 1)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paymentId").value(1))
                .andExpect(jsonPath("$.paymentStatus").value("IN_PROGRESS"))
                .andExpect(jsonPath("$.order.orderId").value(1));

        verify(paymentService, times(1)).findById(1);
    }
    
    @Test
    void shouldSavePayment() throws Exception {
        // Mock data
        OrderDto orderDto = OrderDto.builder()
                .orderId(1)
                .orderStatus("ORDERED")
                .orderDate(LocalDateTime.now())
                .build();

        PaymentDto inputDto = PaymentDto.builder()
                .orderDto(orderDto)
                .build();

        PaymentDto savedDto = PaymentDto.builder()
                .paymentId(1)
                .paymentStatus(com.selimhorri.app.domain.PaymentStatus.NOT_STARTED)
                .orderDto(orderDto)
                .build();

        // Mock service calls
        when(paymentService.save(any(PaymentDto.class))).thenReturn(savedDto);
        when(restTemplate.getForObject(
                eq(AppConstant.DiscoveredDomainsApi.ORDER_SERVICE_API_URL + "/1"), 
                eq(OrderDto.class)))
                .thenReturn(orderDto);

        // Perform request and verify
        mockMvc.perform(post("/api/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(inputDto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paymentId").value(1))
                .andExpect(jsonPath("$.paymentStatus").value("NOT_STARTED"))
                .andExpect(jsonPath("$.order.orderId").value(1));

        verify(paymentService, times(1)).save(any(PaymentDto.class));
    }

    @Test
    void shouldReturnBadRequestWhenOrderNotInCorrectStatus() throws Exception {
        // Mock data
        OrderDto orderDto = OrderDto.builder()
                .orderId(1)
                .orderStatus("IN_PAYMENT") // Estado incorrecto para crear pago
                .build();

        PaymentDto inputDto = PaymentDto.builder()
                .orderDto(orderDto)
                .build();

        // Mock service calls
        when(restTemplate.getForObject(
                eq(AppConstant.DiscoveredDomainsApi.ORDER_SERVICE_API_URL + "/1"), 
                eq(OrderDto.class)))
                .thenReturn(orderDto);
        when(paymentService.save(any(PaymentDto.class)))
                .thenThrow(new IllegalArgumentException(
                        "Cannot start the payment of an order that is not ordered or already in a payment process"));

        // Perform request and verify
        mockMvc.perform(post("/api/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(inputDto)))
                .andExpect(status().isBadRequest());

        verify(paymentService, times(1)).save(any(PaymentDto.class));
    }

    @Test
    void shouldUpdatePaymentStatus() throws Exception {
        // Mock data
        OrderDto orderDto = OrderDto.builder()
                .orderId(1)
                .orderStatus("IN_PAYMENT")
                .build();

        PaymentDto updatedDto = PaymentDto.builder()
                .paymentId(1)
                .paymentStatus(com.selimhorri.app.domain.PaymentStatus.IN_PROGRESS) // Estado actualizado
                .orderDto(orderDto)
                .build();

        // Mock service call
        when(paymentService.updateStatus(anyInt())).thenReturn(updatedDto);

        // Perform request and verify
        mockMvc.perform(patch("/api/payments/{paymentId}", 1)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paymentId").value(1))
                .andExpect(jsonPath("$.paymentStatus").value("IN_PROGRESS"));

        verify(paymentService, times(1)).updateStatus(1);
    }

    @Test
    void shouldReturnBadRequestWhenUpdatingCompletedPayment() throws Exception {
        // Mock service to throw exception
        when(paymentService.updateStatus(anyInt()))
                .thenThrow(new IllegalStateException("Payment is already COMPLETED and cannot be updated further"));

        // Perform request and verify
        mockMvc.perform(patch("/api/payments/{paymentId}", 1)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());

        verify(paymentService, times(1)).updateStatus(1);
    }

    @Test
    void shouldDeletePayment() throws Exception {
        // Mock service (deleteById is void)
        doNothing().when(paymentService).deleteById(anyInt());

        // Perform request and verify
        mockMvc.perform(delete("/api/payments/{paymentId}", 1)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().string("true"));

        verify(paymentService, times(1)).deleteById(1);
    }

    @Test
    void shouldReturnBadRequestWhenDeletingCompletedPayment() throws Exception {
        // Mock service to throw exception
        doThrow(new IllegalArgumentException("Cannot cancel a completed payment"))
                .when(paymentService).deleteById(anyInt());

        // Perform request and verify
        mockMvc.perform(delete("/api/payments/{paymentId}", 1)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());

        verify(paymentService, times(1)).deleteById(1);
    }

    @Test
    void shouldReturnBadRequestWhenBlankPaymentId() throws Exception {
        mockMvc.perform(get("/api/payments/{paymentId}", " ")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());

        verify(paymentService, never()).findById(anyInt());
    }

    @Test
    void shouldReturnBadRequestWhenSaveWithNullPayment() throws Exception {
        mockMvc.perform(post("/api/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content("null"))
                .andExpect(status().isBadRequest());

        verify(paymentService, never()).save(any());
    }
}