package com.github.faizalzafri.kafkaapp.spring.elasticsearch;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TweetRepository extends ElasticsearchRepository<TweetDocument, String> {
}
