package com.selimhorri.app.service;

import java.util.List;

import com.selimhorri.app.dto.PaymentDto;

public interface PaymentService {
	
	List<PaymentDto> findAll();
	PaymentDto findById(final Integer paymentId);
	PaymentDto save(final PaymentDto paymentDto);
	PaymentDto updateStatus(int paymentId);
	void deleteById(final Integer paymentId);
	
}
