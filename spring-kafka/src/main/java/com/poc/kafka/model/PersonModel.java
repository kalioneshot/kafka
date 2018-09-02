package com.poc.kafka.model;

import java.util.Date;
import java.util.StringJoiner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PersonModel extends Model {

	private String name;

	private String lastname;

	private Date date;

	public PersonModel() {
		super();
	}

	@JsonCreator
	public PersonModel(@JsonProperty("name") String name, @JsonProperty("lastname") String lastname) {
		super();
		this.name = name;
		this.lastname = lastname;
		this.date = new Date();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", PersonModel.class.getSimpleName() + "[", "]").add("name=" + name)
				.add("lastname=" + lastname).toString();
	}
}
