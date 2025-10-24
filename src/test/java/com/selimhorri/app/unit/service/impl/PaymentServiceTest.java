package com.selimhorri.app.unit.service.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.selimhorri.app.domain.Payment;
import com.selimhorri.app.domain.PaymentStatus;
import com.selimhorri.app.domain.enums.OrderStatus;
import com.selimhorri.app.dto.OrderDto;
import com.selimhorri.app.dto.PaymentDto;
import com.selimhorri.app.exception.wrapper.PaymentNotFoundException;
import com.selimhorri.app.exception.wrapper.PaymentServiceException;
import com.selimhorri.app.repository.PaymentRepository;
import com.selimhorri.app.service.impl.PaymentServiceImpl;

@ExtendWith(MockitoExtension.class)
class PaymentServiceImplTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private RestTemplate restTemplate;

    @InjectMocks
    private PaymentServiceImpl paymentService;

    private Payment payment;
    private PaymentDto paymentDto;
    private OrderDto orderDto;

    @BeforeEach
    void setUp() {
        orderDto = OrderDto.builder()
                .orderId(1)
                .orderDate(LocalDateTime.now())
                .orderDesc("Test Order")
                .orderFee(100.0)
                .orderStatus(OrderStatus.ORDERED.name())
                .build();

        payment = new Payment();
        payment.setPaymentId(1);
        payment.setIsPayed(false);
        payment.setPaymentStatus(PaymentStatus.NOT_STARTED);
        payment.setOrderId(1);

        paymentDto = PaymentDto.builder()
                .paymentId(1)
                .isPayed(false)
                .paymentStatus(PaymentStatus.NOT_STARTED)
                .orderDto(orderDto)
                .build();
    }

    @Test
    void findAll_ShouldReturnPaymentsWithInPaymentOrderStatus() {
        // Given
        OrderDto inPaymentOrder = OrderDto.builder()
                .orderId(1)
                .orderStatus("IN_PAYMENT")
                .build();

        List<Payment> payments = Arrays.asList(payment);
        
        when(paymentRepository.findAll()).thenReturn(payments);
        when(restTemplate.getForObject(anyString(), eq(OrderDto.class)))
                .thenReturn(inPaymentOrder);

        // When
        List<PaymentDto> result = paymentService.findAll();

        // Then
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("IN_PAYMENT", result.get(0).getOrderDto().getOrderStatus());
        verify(paymentRepository).findAll();
        verify(restTemplate).getForObject(contains("/1"), eq(OrderDto.class));
    }

    @Test
    void findAll_ShouldFilterOutPaymentsWithoutInPaymentStatus() {
        // Given
        OrderDto orderedOrder = OrderDto.builder()
                .orderId(1)
                .orderStatus("ORDERED")
                .build();

        List<Payment> payments = Arrays.asList(payment);
        
        when(paymentRepository.findAll()).thenReturn(payments);
        when(restTemplate.getForObject(anyString(), eq(OrderDto.class)))
                .thenReturn(orderedOrder);

        // When
        List<PaymentDto> result = paymentService.findAll();

        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(paymentRepository).findAll();
    }

    @Test
    void findAll_ShouldHandleRestTemplateException() {
        // Given
        List<Payment> payments = Arrays.asList(payment);
        
        when(paymentRepository.findAll()).thenReturn(payments);
        when(restTemplate.getForObject(anyString(), eq(OrderDto.class)))
                .thenThrow(new RestClientException("Service unavailable"));

        // When
        List<PaymentDto> result = paymentService.findAll();

        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(paymentRepository).findAll();
    }

    @Test
    void findById_ShouldReturnPaymentWithOrderData() {
        // Given
        when(paymentRepository.findById(1)).thenReturn(Optional.of(payment));
        when(restTemplate.getForObject(anyString(), eq(OrderDto.class)))
                .thenReturn(orderDto);

        // When
        PaymentDto result = paymentService.findById(1);

        // Then
        assertNotNull(result);
        assertEquals(1, result.getPaymentId());
        assertNotNull(result.getOrderDto());
        assertEquals(1, result.getOrderDto().getOrderId());
        verify(paymentRepository).findById(1);
        verify(restTemplate).getForObject(contains("/1"), eq(OrderDto.class));
    }

    @Test
    void findById_ShouldThrowExceptionWhenPaymentNotFound() {
        // Given
        when(paymentRepository.findById(1)).thenReturn(Optional.empty());

        // When & Then
        PaymentServiceException exception = assertThrows(
                PaymentServiceException.class,
                () -> paymentService.findById(1)
        );
        
        assertTrue(exception.getMessage().contains("Payment with id: 1 not found"));
        verify(paymentRepository).findById(1);
        verify(restTemplate, never()).getForObject(anyString(), eq(OrderDto.class));
    }

    @Test
    void findById_ShouldThrowExceptionWhenOrderServiceFails() {
        // Given
        when(paymentRepository.findById(1)).thenReturn(Optional.of(payment));
        when(restTemplate.getForObject(anyString(), eq(OrderDto.class)))
                .thenThrow(new RestClientException("Service unavailable"));

        // When & Then
        PaymentServiceException exception = assertThrows(
                PaymentServiceException.class,
                () -> paymentService.findById(1)
        );
        
        assertEquals("Could not fetch order information for payment", exception.getMessage());
        verify(paymentRepository).findById(1);
    }

    @Test
    void save_ShouldSavePaymentAndUpdateOrderStatus() {
        // Given
        Payment savedPayment = new Payment();
        savedPayment.setPaymentId(1);
        savedPayment.setIsPayed(false);
        savedPayment.setPaymentStatus(PaymentStatus.NOT_STARTED);
        savedPayment.setOrderId(1);

        when(restTemplate.getForObject(anyString(), eq(OrderDto.class)))
                .thenReturn(orderDto);
        when(paymentRepository.save(any(Payment.class))).thenReturn(savedPayment);
        when(restTemplate.patchForObject(anyString(), isNull(), eq(Void.class)))
                .thenReturn(null);

        // When
        PaymentDto result = paymentService.save(paymentDto);

        // Then
        assertNotNull(result);
        assertEquals(1, result.getPaymentId());
        verify(restTemplate).getForObject(contains("/1"), eq(OrderDto.class));
        verify(paymentRepository).save(any(Payment.class));
        verify(restTemplate).patchForObject(contains("/1/status"), isNull(), eq(Void.class));
    }

    @Test
    void save_ShouldThrowExceptionWhenOrderIdIsNull() {
        // Given
        PaymentDto invalidPaymentDto = PaymentDto.builder()
                .paymentId(1)
                .isPayed(false)
                .paymentStatus(PaymentStatus.NOT_STARTED)
                .orderDto(null)
                .build();

        // When & Then
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> paymentService.save(invalidPaymentDto)
        );
        
        assertEquals("Order ID must not be null", exception.getMessage());
        verify(paymentRepository, never()).save(any());
    }

    @Test
    void save_ShouldThrowExceptionWhenOrderNotFound() {
        // Given
        when(restTemplate.getForObject(anyString(), eq(OrderDto.class)))
                .thenThrow(HttpClientErrorException.NotFound.create(
                        org.springframework.http.HttpStatus.NOT_FOUND,
                        "Not Found",
                        null,
                        null,
                        null
                ));

        // When & Then
        PaymentServiceException exception = assertThrows(
                PaymentServiceException.class,
                () -> paymentService.save(paymentDto)
        );
        
        assertTrue(exception.getMessage().contains("Order with ID 1 not found"));
        verify(paymentRepository, never()).save(any());
    }

    @Test
    void save_ShouldThrowExceptionWhenOrderStatusIsNotOrdered() {
        // Given
        OrderDto inPaymentOrder = OrderDto.builder()
                .orderId(1)
                .orderStatus(OrderStatus.IN_PAYMENT.name())
                .build();

        when(restTemplate.getForObject(anyString(), eq(OrderDto.class)))
                .thenReturn(inPaymentOrder);

        // When & Then
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> paymentService.save(paymentDto)
        );
        
        assertTrue(exception.getMessage().contains("Cannot start the payment of an order"));
        verify(paymentRepository, never()).save(any());
    }

    @Test
    void save_ShouldThrowExceptionWhenOrderStatusUpdateFails() {
        // Given
        Payment savedPayment = new Payment();
        savedPayment.setPaymentId(1);
        savedPayment.setIsPayed(false);
        savedPayment.setPaymentStatus(PaymentStatus.NOT_STARTED);
        savedPayment.setOrderId(1);

        when(restTemplate.getForObject(anyString(), eq(OrderDto.class)))
                .thenReturn(orderDto);
        when(paymentRepository.save(any(Payment.class))).thenReturn(savedPayment);
        when(restTemplate.patchForObject(anyString(), isNull(), eq(Void.class)))
                .thenThrow(new RestClientException("Update failed"));

        // When & Then
        PaymentServiceException exception = assertThrows(
                PaymentServiceException.class,
                () -> paymentService.save(paymentDto)
        );
        
        assertTrue(exception.getMessage().contains("Payment saved but failed to update order status"));
        verify(paymentRepository).save(any(Payment.class));
    }

    @Test
    void updateStatus_ShouldUpdateFromNotStartedToInProgress() {
        // Given
        Payment updatedPayment = new Payment();
        updatedPayment.setPaymentId(1);
        updatedPayment.setPaymentStatus(PaymentStatus.IN_PROGRESS);

        when(paymentRepository.findById(1)).thenReturn(Optional.of(payment));
        when(paymentRepository.save(any(Payment.class))).thenReturn(updatedPayment);

        // When
        PaymentDto result = paymentService.updateStatus(1);

        // Then
        assertNotNull(result);
        assertEquals(PaymentStatus.IN_PROGRESS, result.getPaymentStatus());
        verify(paymentRepository).findById(1);
        verify(paymentRepository).save(any(Payment.class));
    }

    @Test
    void updateStatus_ShouldUpdateFromInProgressToCompleted() {
        // Given
        payment.setPaymentStatus(PaymentStatus.IN_PROGRESS);
        Payment updatedPayment = new Payment();
        updatedPayment.setPaymentId(1);
        updatedPayment.setPaymentStatus(PaymentStatus.COMPLETED);

        when(paymentRepository.findById(1)).thenReturn(Optional.of(payment));
        when(paymentRepository.save(any(Payment.class))).thenReturn(updatedPayment);

        // When
        PaymentDto result = paymentService.updateStatus(1);

        // Then
        assertNotNull(result);
        assertEquals(PaymentStatus.COMPLETED, result.getPaymentStatus());
        verify(paymentRepository).save(any(Payment.class));
    }

    @Test
    void updateStatus_ShouldThrowExceptionWhenPaymentCompleted() {
        // Given
        payment.setPaymentStatus(PaymentStatus.COMPLETED);
        when(paymentRepository.findById(1)).thenReturn(Optional.of(payment));

        // When & Then
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> paymentService.updateStatus(1)
        );
        
        assertTrue(exception.getMessage().contains("Payment is already COMPLETED"));
        verify(paymentRepository, never()).save(any());
    }

    @Test
    void updateStatus_ShouldThrowExceptionWhenPaymentCanceled() {
        // Given
        payment.setPaymentStatus(PaymentStatus.CANCELED);
        when(paymentRepository.findById(1)).thenReturn(Optional.of(payment));

        // When & Then
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> paymentService.updateStatus(1)
        );
        
        assertTrue(exception.getMessage().contains("Payment is CANCELED"));
        verify(paymentRepository, never()).save(any());
    }

    @Test
    void updateStatus_ShouldThrowExceptionWhenPaymentNotFound() {
        // Given
        when(paymentRepository.findById(1)).thenReturn(Optional.empty());

        // When & Then
        PaymentNotFoundException exception = assertThrows(
                PaymentNotFoundException.class,
                () -> paymentService.updateStatus(1)
        );
        
        assertTrue(exception.getMessage().contains("Payment with id: 1 not found"));
        verify(paymentRepository, never()).save(any());
    }

    @Test
    void deleteById_ShouldCancelPayment() {
        // Given
        when(paymentRepository.findById(1)).thenReturn(Optional.of(payment));
        when(paymentRepository.save(any(Payment.class))).thenReturn(payment);

        // When
        paymentService.deleteById(1);

        // Then
        verify(paymentRepository).findById(1);
        verify(paymentRepository).save(argThat(p -> p.getPaymentStatus() == PaymentStatus.CANCELED));
    }

    @Test
    void deleteById_ShouldThrowExceptionWhenPaymentNotFound() {
        // Given
        when(paymentRepository.findById(1)).thenReturn(Optional.empty());

        // When & Then
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> paymentService.deleteById(1)
        );
        
        assertTrue(exception.getMessage().contains("Payment with id 1 not found"));
        verify(paymentRepository, never()).save(any());
    }

    @Test
    void deleteById_ShouldThrowExceptionWhenPaymentCompleted() {
        // Given
        payment.setPaymentStatus(PaymentStatus.COMPLETED);
        when(paymentRepository.findById(1)).thenReturn(Optional.of(payment));

        // When & Then
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> paymentService.deleteById(1)
        );
        
        assertTrue(exception.getMessage().contains("Cannot cancel a completed payment"));
        verify(paymentRepository, never()).save(any());
    }

    @Test
    void deleteById_ShouldThrowExceptionWhenPaymentAlreadyCanceled() {
        // Given
        payment.setPaymentStatus(PaymentStatus.CANCELED);
        when(paymentRepository.findById(1)).thenReturn(Optional.of(payment));

        // When & Then
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> paymentService.deleteById(1)
        );
        
        assertTrue(exception.getMessage().contains("Payment is already canceled"));
        verify(paymentRepository, never()).save(any());
    }
}