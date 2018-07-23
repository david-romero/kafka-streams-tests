package com.davromalc.kafka.streams.model.menu;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.davromalc.kafka.streams.model.menu.Food;
import com.davromalc.kafka.streams.model.menu.Ingredient;
import com.davromalc.kafka.streams.model.menu.Menu;
import com.davromalc.kafka.streams.model.menu.Subscriptor;

public class MenuBuilder {

	private List<Food> foods = new ArrayList<>();

	private Subscriptor subscriptor;
	
	public MenuBuilder withFood(Food food) {
		foods.add(food);
		return this;
	}
	
	public MenuBuilder withIngredientAndCurrentDate(Ingredient ingredient) {
		Food food = new Food(LocalDateTime.now());
		food.getIngredients().add(ingredient);
		return withFood(food);
	}
	
	public MenuBuilder withSubscriptor(Subscriptor subscriptor) {
		this.subscriptor = subscriptor;
		return this;
	}
	
	public MenuBuilder withSubscriptor(String name, String email) {
		this.subscriptor = new Subscriptor(null, email, name, null);
		return this;
	}
	
	public MenuBuilder withFoods(Collection<Food> foods) {
		this.foods.addAll(foods);
		return this;
	}

	public Menu build() {
		final Menu menu = new Menu();
		menu.setFoods(foods);
		menu.setSubscriptor(subscriptor);
		return menu;
	}

}
