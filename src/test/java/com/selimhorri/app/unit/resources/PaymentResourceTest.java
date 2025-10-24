package com.selimhorri.app.unit.resources;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.selimhorri.app.domain.PaymentStatus;
import com.selimhorri.app.dto.OrderDto;
import com.selimhorri.app.dto.PaymentDto;
import com.selimhorri.app.dto.response.collection.DtoCollectionResponse;
import com.selimhorri.app.exception.wrapper.PaymentNotFoundException;
import com.selimhorri.app.exception.wrapper.PaymentServiceException;
import com.selimhorri.app.resource.PaymentResource;
import com.selimhorri.app.service.PaymentService;

@ExtendWith(MockitoExtension.class)
class PaymentResourceTest {

    @Mock
    private PaymentService paymentService;

    @InjectMocks
    private PaymentResource paymentResource;

    private PaymentDto paymentDto;
    private OrderDto orderDto;
    private List<PaymentDto> paymentList;

    @BeforeEach
    void setUp() {
        orderDto = OrderDto.builder()
                .orderId(1)
                .orderDate(LocalDateTime.now())
                .orderDesc("Test Order")
                .orderFee(100.0)
                .orderStatus("ORDERED")
                .build();

        paymentDto = PaymentDto.builder()
                .paymentId(1)
                .isPayed(false)
                .paymentStatus(PaymentStatus.NOT_STARTED)
                .orderDto(orderDto)
                .build();

        paymentList = Arrays.asList(paymentDto);
    }

    @Test
    void findAll_ShouldReturnAllPayments() {
        // Given
        when(paymentService.findAll()).thenReturn(paymentList);

        // When
        ResponseEntity<DtoCollectionResponse<PaymentDto>> response = paymentResource.findAll();

        // Then
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(1, response.getBody().getCollection().size());
        verify(paymentService).findAll();
    }

    @Test
    void findAll_ShouldReturnEmptyListWhenNoPayments() {
        // Given
        when(paymentService.findAll()).thenReturn(Arrays.asList());

        // When
        ResponseEntity<DtoCollectionResponse<PaymentDto>> response = paymentResource.findAll();

        // Then
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().getCollection().isEmpty());
        verify(paymentService).findAll();
    }

    @Test
    void findById_ShouldReturnPaymentWhenFound() {
        // Given
        String paymentId = "1";
        when(paymentService.findById(1)).thenReturn(paymentDto);

        // When
        ResponseEntity<PaymentDto> response = paymentResource.findById(paymentId);

        // Then
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(paymentDto, response.getBody());
        verify(paymentService).findById(1);
    }

    @Test
    void findById_ShouldThrowExceptionWhenPaymentNotFound() {
        // Given
        String paymentId = "999";
        when(paymentService.findById(999))
                .thenThrow(new PaymentServiceException("Payment with id: 999 not found"));

        // When & Then
        PaymentServiceException exception = assertThrows(
                PaymentServiceException.class,
                () -> paymentResource.findById(paymentId)
        );
        
        assertTrue(exception.getMessage().contains("Payment with id: 999 not found"));
        verify(paymentService).findById(999);
    }

    @Test
    void findById_ShouldThrowExceptionWhenInvalidIdFormat() {
        // Given
        String invalidPaymentId = "invalid";

        // When & Then
        assertThrows(
                NumberFormatException.class,
                () -> paymentResource.findById(invalidPaymentId)
        );
        
        verify(paymentService, never()).findById(anyInt());
    }

    @Test
    void save_ShouldReturnSavedPayment() {
        // Given
        PaymentDto savedPaymentDto = PaymentDto.builder()
                .paymentId(1)
                .isPayed(false)
                .paymentStatus(PaymentStatus.NOT_STARTED)
                .orderDto(orderDto)
                .build();

        when(paymentService.save(paymentDto)).thenReturn(savedPaymentDto);

        // When
        ResponseEntity<PaymentDto> response = paymentResource.save(paymentDto);

        // Then
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(savedPaymentDto, response.getBody());
        verify(paymentService).save(paymentDto);
    }


    @Test
    void save_ShouldThrowExceptionWhenServiceFails() {
        // Given
        when(paymentService.save(paymentDto))
                .thenThrow(new PaymentServiceException("Failed to save payment"));

        // When & Then
        PaymentServiceException exception = assertThrows(
                PaymentServiceException.class,
                () -> paymentResource.save(paymentDto)
        );
        
        assertEquals("Failed to save payment", exception.getMessage());
        verify(paymentService).save(paymentDto);
    }

    @Test
    void updateStatus_ShouldReturnUpdatedPayment() {
        // Given
        String paymentId = "1";
        PaymentDto updatedPaymentDto = PaymentDto.builder()
                .paymentId(1)
                .isPayed(false)
                .paymentStatus(PaymentStatus.IN_PROGRESS)
                .orderDto(orderDto)
                .build();

        when(paymentService.updateStatus(1)).thenReturn(updatedPaymentDto);

        // When
        ResponseEntity<PaymentDto> response = paymentResource.updateStatus(paymentId);

        // Then
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(updatedPaymentDto, response.getBody());
        assertEquals(PaymentStatus.IN_PROGRESS, response.getBody().getPaymentStatus());
        verify(paymentService).updateStatus(1);
    }

    @Test
    void updateStatus_ShouldThrowExceptionWhenPaymentNotFound() {
        // Given
        String paymentId = "999";
        when(paymentService.updateStatus(999))
                .thenThrow(new PaymentNotFoundException("Payment with id: 999 not found"));

        // When & Then
        PaymentNotFoundException exception = assertThrows(
                PaymentNotFoundException.class,
                () -> paymentResource.updateStatus(paymentId)
        );
        
        assertTrue(exception.getMessage().contains("Payment with id: 999 not found"));
        verify(paymentService).updateStatus(999);
    }

    @Test
    void updateStatus_ShouldThrowExceptionWhenInvalidIdFormat() {
        // Given
        String invalidPaymentId = "invalid";

        // When & Then
        assertThrows(
                NumberFormatException.class,
                () -> paymentResource.updateStatus(invalidPaymentId)
        );
        
        verify(paymentService, never()).updateStatus(anyInt());
    }

    @Test
    void updateStatus_ShouldThrowExceptionWhenStatusCannotBeUpdated() {
        // Given
        String paymentId = "1";
        when(paymentService.updateStatus(1))
                .thenThrow(new IllegalStateException("Payment is already COMPLETED"));

        // When & Then
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> paymentResource.updateStatus(paymentId)
        );
        
        assertTrue(exception.getMessage().contains("Payment is already COMPLETED"));
        verify(paymentService).updateStatus(1);
    }

    @Test
    void deleteById_ShouldReturnTrueWhenDeleted() {
        // Given
        String paymentId = "1";
        doNothing().when(paymentService).deleteById(1);

        // When
        ResponseEntity<Boolean> response = paymentResource.deleteById(paymentId);

        // Then
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(response.getBody());
        verify(paymentService).deleteById(1);
    }

    @Test
    void deleteById_ShouldThrowExceptionWhenPaymentNotFound() {
        // Given
        String paymentId = "999";
        doThrow(new IllegalArgumentException("Payment with id 999 not found"))
                .when(paymentService).deleteById(999);

        // When & Then
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> paymentResource.deleteById(paymentId)
        );
        
        assertTrue(exception.getMessage().contains("Payment with id 999 not found"));
        verify(paymentService).deleteById(999);
    }

    @Test
    void deleteById_ShouldThrowExceptionWhenInvalidIdFormat() {
        // Given
        String invalidPaymentId = "invalid";

        assertThrows(
                NumberFormatException.class,
                () -> paymentResource.deleteById(invalidPaymentId)
        );
        
        verify(paymentService, never()).deleteById(anyInt());
    }

    @Test
    void deleteById_ShouldThrowExceptionWhenPaymentCannotBeDeleted() {
        // Given
        String paymentId = "1";
        doThrow(new IllegalArgumentException("Cannot cancel a completed payment"))
                .when(paymentService).deleteById(1);

        // When & Then
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> paymentResource.deleteById(paymentId)
        );
        
        assertTrue(exception.getMessage().contains("Cannot cancel a completed payment"));
        verify(paymentService).deleteById(1);
    }

    @Test
    void deleteById_ShouldHandleNullPaymentId() {
        // Given
        String nullPaymentId = null;

        assertThrows(
                NumberFormatException.class,
                () -> paymentResource.deleteById(nullPaymentId)
        );
        
        verify(paymentService, never()).deleteById(anyInt());
    }

    @Test
    void deleteById_ShouldHandleEmptyPaymentId() {
        // Given
        String emptyPaymentId = "";

        assertThrows(
                NumberFormatException.class,
                () -> paymentResource.deleteById(emptyPaymentId)
        );
        
        verify(paymentService, never()).deleteById(anyInt());
    }
}