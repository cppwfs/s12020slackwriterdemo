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

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.client.RestTemplate;

@EnableBatchProcessing
@Configuration
@EnableTask
@EnableConfigurationProperties(BatchSlackProperties.class)
public class BatchSlackConfiguration {

	@Autowired
	private BatchSlackProperties properties;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Bean
	public Job processPurchaseOrders(Step step) {
		return jobBuilderFactory.get("processPurchaseOrders").incrementer(new RunIdIncrementer()).flow(step).end().build();
	}

	@Bean
	public Step step1(ItemReader itemReader,  ItemWriter itemWriter) {
		return stepBuilderFactory.get("step1").<Map<Object, Object>, Map<Object, Object>>chunk(10).reader(itemReader)
				.writer(itemWriter).build();
	}

	@Bean
	public ItemReader<Map<Object, Object>> itemReader(DataSource dataSource, RowMapper rowMapper) {
		return new JdbcCursorItemReaderBuilder<Map<Object, Object>>()
				.sql("SELECT user_id, sku, quantity, amount, mode FROM purchase_orders").
				name("purchase_order_reader").rowMapper(rowMapper)
				.dataSource(dataSource).build();
	}

	@Bean
	public RowMapper<Map<Object, Object>> rowMapper() {
		return new MapRowMapper();
	}

	@Bean
	public ItemWriter<Map<Object, Object>> itemWriter() {
		return new ItemWriter<Map<Object, Object>>() {
			@Override
			public void write(List<? extends Map<Object, Object>> items) throws Exception {
				for (Map<Object, Object> item : items) {
					if ( ((Integer)item.get("quantity")).doubleValue() > 2) {
						RestTemplate restTemplate = new RestTemplate();
						String alertMessage = String.format("{\"text\":\"SKU %s was purchased %s times today\"}", item.get("sku"), item.get("quantity"));
						restTemplate.postForEntity(properties.getUrl(), alertMessage, null);
					}
				}
			}
		};
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
