package com.ncst.impl;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.ncst.rqlite.Rqlite;
import com.ncst.dto.QueryResults;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class QueryRequest extends GenericRequest {

    private HttpRequest httpRequest;

    public QueryRequest(HttpRequest request) {
        this.httpRequest = request;
    }

    @Override
    public QueryResults execute() throws IOException {
        HttpResponse response = this.httpRequest.execute();
        return response.parseAs(QueryResults.class);
    }

    @Override
    public String getUrl() {
        return this.httpRequest.getUrl().toString();
    }

    @Override
    public void setUrl(GenericUrl url) {
        this.httpRequest.setUrl(url);
    }

    public String getMethod() {
        return this.httpRequest.getRequestMethod();
    }

    public String getBody() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        this.httpRequest.getContent().writeTo(stream);
        return stream.toString();
    }

    public void writeContentTo(OutputStream out) {
        return;
    }

    public QueryRequest setReadConsistencyLevel(Rqlite.ReadConsistencyLevel lvl) {
        this.httpRequest.getUrl().put("level", lvl.toString().toLowerCase());
        return this;
    }

    public QueryRequest enableTransaction(Boolean tx) {
        if (tx) {
            this.httpRequest.getUrl().put("transaction", tx.toString());
        } else {
            this.httpRequest.getUrl().remove("transaction");
        }
        return this;
    }

    public QueryRequest enableTimings(Boolean tm) {
        if (tm) {
            this.httpRequest.getUrl().put("timings", tm.toString());
        } else {
            this.httpRequest.getUrl().remove("timings");
        }
        return this;
    }
}