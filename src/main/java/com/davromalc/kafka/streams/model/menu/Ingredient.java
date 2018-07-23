package com.davromalc.kafka.streams.model.menu;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class Ingredient {

	private String name;
	
	private String technicalName;
	
	private double price;
	
	private String url;
	
	private String provider;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((technicalName == null) ? 0 : technicalName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Ingredient other = (Ingredient) obj;
		if (technicalName == null) {
			if (other.technicalName != null)
				return false;
		} else if (!technicalName.equals(other.technicalName))
			return false;
		return true;
	}
	
	
	
}
