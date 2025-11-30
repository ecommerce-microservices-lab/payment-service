package com.selimhorri.app.service.impl;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.transaction.Transactional;

import io.github.resilience4j.retry.annotation.Retry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.selimhorri.app.constant.AppConstant;
import com.selimhorri.app.domain.Payment;
import com.selimhorri.app.domain.PaymentStatus;
import com.selimhorri.app.domain.enums.OrderStatus;
import com.selimhorri.app.dto.OrderDto;
import com.selimhorri.app.dto.PaymentDto;
import com.selimhorri.app.exception.wrapper.PaymentNotFoundException;
import com.selimhorri.app.exception.wrapper.PaymentServiceException;
import com.selimhorri.app.helper.PaymentMappingHelper;
import com.selimhorri.app.repository.PaymentRepository;
import com.selimhorri.app.service.PaymentService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Transactional
@Slf4j
@RequiredArgsConstructor
public class PaymentServiceImpl implements PaymentService {

	private final PaymentRepository paymentRepository;
	private final RestTemplate restTemplate;
    private final MeterRegistry meterRegistry;

    private Counter completedPayments;

    @PostConstruct
    public void initMetric(){
        this.completedPayments = Counter
                .builder("completed_payments_total")
                .description("Pagos completados exitosamente")
                .tag("service", "payment")
                .register(meterRegistry);
    }

	@Override
	public List<PaymentDto> findAll() {
		log.info("*** PaymentDto List, service; fetch payments with order status IN_PAYMENT *");

		return this.paymentRepository.findAll()
				.stream()
				.map(PaymentMappingHelper::map)
				.filter(p -> {
					try {
						OrderDto orderDto = this.restTemplate.getForObject(
								AppConstant.DiscoveredDomainsApi.ORDER_SERVICE_API_URL + "/"
										+ p.getOrderDto().getOrderId(),
								OrderDto.class);

						// Verificar si la orden tiene estado IN_PAYMENT
						boolean isInPayment = "IN_PAYMENT".equalsIgnoreCase(orderDto.getOrderStatus());
						if (isInPayment) {
							p.setOrderDto(orderDto);
							return true;
						}
						return false;

					} catch (Exception e) {
						log.error("Error fetching order for payment ID {}: {}", p.getPaymentId(), e.getMessage());
						return false;
					}
				})
				.distinct()
				.collect(Collectors.toUnmodifiableList());
	}

	@Override
	public PaymentDto findById(final Integer paymentId) {
		log.info("*** PaymentDto, service; fetch payment by id *");
		PaymentDto paymentDto = this.paymentRepository.findById(paymentId)
				.map(PaymentMappingHelper::map)
				.orElseThrow(
						() -> new PaymentServiceException(String.format("Payment with id: %d not found", paymentId)));

		try {
			OrderDto orderDto = this.restTemplate.getForObject(
					AppConstant.DiscoveredDomainsApi.ORDER_SERVICE_API_URL + "/"
							+ paymentDto.getOrderDto().getOrderId(),
					OrderDto.class);
			paymentDto.setOrderDto(orderDto);
			return paymentDto;
		} catch (Exception e) {
			log.error("Error fetching order for payment ID {}: {}", paymentId, e.getMessage());
			throw new PaymentServiceException("Could not fetch order information for payment");
		}
	}

	@Override
	@Transactional
    @Retry(name = "paymentService", fallbackMethod = "saveFallback")
	public PaymentDto save(final PaymentDto paymentDto) {
		log.info("*** PaymentDto, service; save payment *");

		// Verificar que la orden existe antes de guardar el pago
		if (paymentDto.getOrderDto() == null || paymentDto.getOrderDto().getOrderId() == null) {
			throw new IllegalArgumentException("Order ID must not be null");
		}

		try {
			// 1. Verificar existencia de la orden
			// Las excepciones RestClientException y ResourceAccessException se propagan
			// para que Resilience4j pueda detectarlas y hacer retry
			OrderDto orderDto = this.restTemplate.getForObject(
					AppConstant.DiscoveredDomainsApi.ORDER_SERVICE_API_URL + "/"
							+ paymentDto.getOrderDto().getOrderId(),
					OrderDto.class);

			if (orderDto == null) {
				throw new PaymentServiceException(
						"Order with ID " + paymentDto.getOrderDto().getOrderId() + " not found");
			}
			if (!orderDto.getOrderStatus().equals(OrderStatus.ORDERED.name())) {
				throw new IllegalArgumentException(
						"Cannot start the payment of an order that is not ordered or already in a payment process");
			}
			// 2. Guardar el pago
			PaymentDto savedPayment = PaymentMappingHelper.map(
					this.paymentRepository.save(PaymentMappingHelper.mapForPayment(paymentDto)));

			// 3. Actualizar estado de la orden (PATCH)
			// Las excepciones RestClientException se propagan para que Resilience4j pueda hacer retry
			String patchUrl = AppConstant.DiscoveredDomainsApi.ORDER_SERVICE_API_URL + "/"
					+ paymentDto.getOrderDto().getOrderId() + "/status";

			this.restTemplate.patchForObject(
					patchUrl,
					null,
					Void.class);
			log.info("Order status updated successfully for order ID: {}", paymentDto.getOrderDto().getOrderId());

			return savedPayment;

		} catch (HttpClientErrorException.NotFound ex) {
			// Esta excepción está en ignore-exceptions, no se reintentará
			log.error("Order not found for order ID: {}", paymentDto.getOrderDto().getOrderId(), ex);
			throw new PaymentServiceException("Order with ID " + paymentDto.getOrderDto().getOrderId() + " not found");
		}
	}

	/**
	 * Método fallback que se ejecuta cuando todos los reintentos del método save han fallado.
	 * 
	 * @param paymentDto el DTO del pago que se intentó guardar
	 * @param ex la excepción que causó el fallback
	 * @return nunca retorna, siempre lanza una excepción
	 * @throws PaymentServiceException siempre lanza esta excepción indicando que no se pudo procesar el pago
	 */
	public PaymentDto saveFallback(final PaymentDto paymentDto, final Exception ex) {
		log.error("*** PaymentDto, service; save payment fallback after all retry attempts failed for order ID: {} *", 
				paymentDto != null && paymentDto.getOrderDto() != null ? paymentDto.getOrderDto().getOrderId() : "unknown", ex);
		throw new PaymentServiceException(
				String.format("Failed to save payment after all retry attempts. Order ID: %s. Error: %s",
						paymentDto != null && paymentDto.getOrderDto() != null 
								? paymentDto.getOrderDto().getOrderId().toString() 
								: "unknown",
						ex.getMessage()));
	}

	@Override
	public PaymentDto updateStatus(final int paymentId) {
		log.info("*** PaymentDto, service; update payment status *");

		return this.paymentRepository.findById(paymentId)
				.map(payment -> {
					PaymentStatus currentStatus = payment.getPaymentStatus();
					PaymentStatus newStatus;
					switch (currentStatus) {
						case NOT_STARTED:
							newStatus = PaymentStatus.IN_PROGRESS;
							break;
						case IN_PROGRESS:
							newStatus = PaymentStatus.COMPLETED;
                            completedPayments.increment();
							break;
						case COMPLETED:
							throw new IllegalStateException(
									"Payment is already COMPLETED and cannot be updated further");
						case CANCELED:
							throw new IllegalStateException("Payment is CANCELED and cannot be updated");
						default:
							throw new IllegalStateException("Unknown payment status: " + currentStatus);
					}

					payment.setPaymentStatus(newStatus);

					return PaymentMappingHelper.map(this.paymentRepository.save(payment));
				})
				.orElseThrow(() -> new PaymentNotFoundException("Payment with id: " + paymentId + " not found"));
	}

	@Override
	@Transactional
	public void deleteById(final Integer paymentId) {
		log.info("*** Void, service; soft delete (cancel) payment by id *");

		Payment payment = this.paymentRepository.findById(paymentId)
				.orElseThrow(() -> new IllegalArgumentException("Payment with id " + paymentId + " not found"));

		if (payment.getPaymentStatus() == PaymentStatus.COMPLETED) {
			log.info("Payment with id {} is COMPLETED and cannot be canceled", paymentId);
			throw new IllegalArgumentException("Cannot cancel a completed payment");
		}

		if (payment.getPaymentStatus() == PaymentStatus.CANCELED) {
			log.info("Payment with id {} is already CANCELED", paymentId);
			throw new IllegalArgumentException("Payment is already canceled");
		}

		payment.setPaymentStatus(PaymentStatus.CANCELED);
		this.paymentRepository.save(payment);
		log.info("Payment with id {} has been canceled", paymentId);
	}
}
