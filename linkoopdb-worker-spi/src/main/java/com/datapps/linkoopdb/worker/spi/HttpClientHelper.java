package com.datapps.linkoopdb.worker.spi;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

public class HttpClientHelper {

    public static String get(String urlParam) throws IOException {
        // 创建httpClient实例对象
        HttpClient httpClient = new HttpClient();
        // 创建post请求方法实例对象
        HttpMethod httpMethod = new GetMethod(urlParam);
        // 设置post请求超时时间
        httpMethod.addRequestHeader("Content-Type", "application/json");

        httpClient.executeMethod(httpMethod);
        String result = httpMethod.getResponseBodyAsString();
        httpMethod.releaseConnection();
        return result;
    }
    public static String put(String url, String body) throws IOException {
        // 创建httpClient实例对象
        HttpClient httpClient = new HttpClient();
        // 设置httpClient连接主机服务器超时时间：15000毫秒
        httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(15000);
        // 创建post请求方法实例对象
        PutMethod httpMethod = new PutMethod(url);
        httpMethod.setRequestEntity(new StringRequestEntity(body, "application/json", "UTF-8"));

        httpClient.executeMethod(httpMethod);
        String result = httpMethod.getResponseBodyAsString();
        httpMethod.releaseConnection();
        return result;
    }
    public static String post(String url, NameValuePair[] requestBody) throws IOException {
        // 创建httpClient实例对象
        HttpClient httpClient = new HttpClient();
        // 设置httpClient连接主机服务器超时时间：15000毫秒
        httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(15000);
        // 创建post请求方法实例对象
        PostMethod httpMethod = new PostMethod(url);
        httpMethod.setRequestBody(requestBody);
        // 设置post请求超时时间
        httpMethod.addRequestHeader("Content-Type", "application/json");

        httpClient.executeMethod(httpMethod);
        String result = httpMethod.getResponseBodyAsString();
        httpMethod.releaseConnection();
        return result;
    }
}
