package com.davromalc.kafka.streams.model.menu;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Setter
public class Food {

	private final List<Ingredient> ingredients = new ArrayList<>();
	
	private LocalDateTime date;
	
}
