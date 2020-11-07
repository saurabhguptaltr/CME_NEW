package com.synechron.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class Sender {
	@Autowired
	private BinderAwareChannelResolver resolver;

	@Transactional
	public void send(String message, String target) {
		MessageChannel destination = resolver.resolveDestination(target);
		destination.send(MessageBuilder.withPayload(message).build());
	}

}
