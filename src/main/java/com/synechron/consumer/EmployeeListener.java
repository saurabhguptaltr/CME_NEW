package com.synechron.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.synechron.model.Employee;
import com.synechron.producer.Sender;

@Component
@EnableBinding(Sink.class)
public class EmployeeListener {

	@Autowired
	Sender sender;

	@StreamListener(value = Sink.INPUT, condition = "headers['kafka_receivedTopic']=='${xmlinput}'")
	@Transactional
	public void consumeXmlMessage(Message<String> message) {

		try {
			ObjectMapper mapper = new ObjectMapper();
			Employee emp = mapper.readValue(message.getPayload(), Employee.class);
			String json = mapper.writeValueAsString(emp);
			sender.send(json, "d1");
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			sender.send(message.getPayload() + " " + e.getMessage(), "d2");
		}

	}
 
	@StreamListener(value = Sink.INPUT, condition = "headers['kafka_receivedTopic']=='${jsoninput}'")
	@Transactional
	public void consumeJsonMessage(Message<String> message) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			String[] csv = message.getPayload().split(",");
			// name, age, address
			Employee emp = new Employee(csv[0], Integer.parseInt(csv[1]), csv[2]);
			String json = mapper.writeValueAsString(emp);
			sender.send(json, "d1");
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			sender.send(message.getPayload() + " " + e.getMessage(), "d2");
		}

	}

}
