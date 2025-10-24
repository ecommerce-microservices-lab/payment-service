package com.selimhorri.app.helper;

import com.selimhorri.app.domain.Payment;
import com.selimhorri.app.domain.PaymentStatus;
import com.selimhorri.app.dto.OrderDto;
import com.selimhorri.app.dto.PaymentDto;

public interface PaymentMappingHelper {

	public static PaymentDto map(final Payment payment) {
		return PaymentDto.builder()
				.paymentId(payment.getPaymentId())
				.isPayed(payment.getIsPayed())
				.paymentStatus(payment.getPaymentStatus())
				.orderDto(
						OrderDto.builder()
								.orderId(payment.getOrderId())
								.build())
				.build();
	}

	public static Payment map(final PaymentDto paymentDto) {
		return Payment.builder()
				.paymentId(paymentDto.getPaymentId())
				.orderId(paymentDto.getOrderDto().getOrderId())
				.isPayed(paymentDto.getIsPayed())
				.paymentStatus(paymentDto.getPaymentStatus())
				.build();
	}

	public static Payment mapForPayment(final PaymentDto paymentDto) {
		return Payment.builder()
				.orderId(paymentDto.getOrderDto().getOrderId())
				.isPayed(false)
				.paymentStatus(PaymentStatus.NOT_STARTED)
				.build();
	}

}
