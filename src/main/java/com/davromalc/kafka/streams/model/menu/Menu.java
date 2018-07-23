package com.davromalc.kafka.streams.model.menu;

import java.time.Period;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class Menu {

	private List<Food> foods = new ArrayList<>();
	
	private Subscriptor subscriptor;
	
	@JsonIgnore
	private Period period;
	
}
