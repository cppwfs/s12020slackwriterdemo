/*
 * Copyright 2020 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.spring.batchlab.configuration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.client.RestTemplate;

@Configuration
@EnableTask
@EnableConfigurationProperties(PurchaseOrderSlackProperties.class)
public class PurchaseOrderSlackConfiguration {

	@Autowired
	private DataSource dataSource;

	@Autowired
	PurchaseOrderSlackProperties properties;

	@Bean
	public ApplicationRunner applicationRunner(RowMapper<Map<Object, Object>> rowMapper) {
		return new ApplicationRunner() {
			@Override
			public void run(ApplicationArguments args) throws Exception {
				postSlackMessage(retrievePOData(rowMapper));
			}
		};
	}

	@Bean
	public RowMapper<Map<Object, Object>> rowMapper() {
		return new MapRowMapper();
	}

	private List<Map<Object, Object>> retrievePOData(RowMapper<Map<Object, Object>> rowMapper) {
		JdbcTemplate jdbcTemplate = new JdbcTemplate(this.dataSource);
		return jdbcTemplate.query("SELECT user_id, sku, quantity, amount, mode " +
				"FROM purchase_orders WHERE quantity > " + this.properties.getQuantityLimit(),
				rowMapper);
	}

	public void postSlackMessage(List<Map<Object, Object>> purchaseOrders) {
		for (Map<Object, Object> item : purchaseOrders) {
			RestTemplate restTemplate = new RestTemplate();
			String alertMessage = String.format("{\"text\":\"PO with SKU %s had a quantity ordered greater than %s\"}",
					item.get("sku"),
					this.properties.getQuantityLimit());
			restTemplate.postForEntity(this.properties.getUrl(), alertMessage, null);
		}
	}

	public static class MapRowMapper implements RowMapper<Map<Object, Object>> {

		@Override
		public Map<Object, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
			Map<Object, Object> item = new HashMap<>(rs.getMetaData().getColumnCount());
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				item.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
			}
			return item;
		}

	}
}
