package com.github.faizalzafri.kafkaapp.spring.elasticsearch;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName = "spring_tweets")
public class TweetDocument {
    @Id
    private String id;

    @Field(type = FieldType.Text)
    private String text;

    @Field(type = FieldType.Integer)
    private int followersCount;

    public TweetDocument() {}

    public TweetDocument(String id, String text, int followersCount) {
        this.id = id;
        this.text = text;
        this.followersCount = followersCount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public int getFollowersCount() {
        return followersCount;
    }

    public void setFollowersCount(int followersCount) {
        this.followersCount = followersCount;
    }
}
