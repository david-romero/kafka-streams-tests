package com.davromalc.kafka.streams.core.kafka.menu;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.kafka.streams.kstream.Reducer;

import com.davromalc.kafka.streams.model.menu.Food;
import com.davromalc.kafka.streams.model.menu.Ingredient;
import com.davromalc.kafka.streams.model.menu.Menu;

public class FoodReducer implements Reducer<Menu> {

	@Override
	public Menu apply(Menu value1, Menu value2) {
		final Menu menu = new Menu();
		menu.setSubscriptor(value1.getSubscriptor());
		menu.setPeriod(value1.getPeriod());
		final List<Food> foods = new ArrayList<>(value1.getFoods());
		final List<Ingredient> ingredients = new ArrayList<>();
		Iterator<Food> foodsIterator = foods.iterator();
		while ( foodsIterator.hasNext() ) {
			ingredients.addAll(foodsIterator.next().getIngredients());
		}
		Iterator<Food> foodsToAddIterator = value2.getFoods().iterator();
		while ( foodsToAddIterator.hasNext() ) {
			final Food foodToAdd = foodsToAddIterator.next();
			foods.add(foodToAdd);
			ListIterator<Ingredient> ingredientsToAdd = foodToAdd.getIngredients().listIterator();
			while ( ingredientsToAdd.hasNext() ) {
				final Ingredient candidateIngredient = ingredientsToAdd.next();
				if (ingredients.contains(candidateIngredient)) {
					ingredientsToAdd.remove();
				}
			}
		}
		foods.sort(Comparator.comparing(Food::getDate));
		menu.setFoods(foods);
		return menu;
	}

}
