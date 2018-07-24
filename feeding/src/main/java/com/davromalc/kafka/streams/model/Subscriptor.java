package com.davromalc.kafka.streams.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Subscriptor {

	private String id;
	
	private String email;
	
	private String name;
	
	private String surname;
	
}
