package com.ncst.rqlite;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.ncst.rqlite.dto.ExecuteResults;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ExecuteRequest extends GenericRequest {

    private HttpRequest httpRequest;

    public ExecuteRequest(HttpRequest request) {
        this.httpRequest = request;
    }

    @Override
    public ExecuteResults execute() throws IOException {
        HttpResponse response = this.httpRequest.execute();
        return response.parseAs(ExecuteResults.class);
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

    public ExecuteRequest enableTransaction(Boolean tx) {
        if (tx) {
            this.httpRequest.getUrl().put("transaction", tx.toString());
        } else {
            this.httpRequest.getUrl().remove("transaction");
        }
        return this;
    }

    public ExecuteRequest enableTimings(Boolean tm) {
        if (tm) {
            this.httpRequest.getUrl().put("timings", tm.toString());
        } else {
            this.httpRequest.getUrl().remove("timings");
        }
        return this;
    }
}
