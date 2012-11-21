/*
 * Copyright 2009 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.linkedin.databus.tests.inprocess;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.util.CharsetUtil;

/**
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author Andy Taylor (andy.taylor@jboss.org)
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 *
 * @version $Rev: 2368 $, $Date: 2010-10-18 17:19:03 +0900 (Mon, 18 Oct 2010) $
 */
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

    private HttpRequest request;
    private boolean readingChunks;
    /** Buffer that stores the response content */
    private final StringBuilder buf = new StringBuilder();

    private static RandomProducerConsumer _controller = new RandomProducerConsumer();
    
    public static final String START_PRODUCER = "/producer/start";
    public static final String STOP_PRODUCER  =  "/producer/stop";
    public static final String SUSPEND_PRODUCER  =  "/producer/suspend";
    public static final String IS_PRODUCER_RUNNING = "/producer/isrunning";
    public static final String GET_PRODUCER_RATE  = "/producer/rate";
    public static final String START_CONSUMER = "/consumer/start";
    public static final String STOP_CONSUMER = "/consumer/stop";
    public static final String IS_CONSUMER_RUNNING = "/consumer/isrunning";
    public static final String TEST_CONSUMER     = "/consumer/test";
    
    public static final String CONSUMER_ID_KEY ="id";
    public static final String CONFIG_FILE_KEY = "config";
    public static final String CONFIG_DIR = 
     "integration-test/config/inprocess/";
    
    public String processRequest(HttpRequest request)
          throws Exception
    {    	
        QueryStringDecoder queryStringDecoder = 
        	                      new QueryStringDecoder(request.getUri());
        Map<String, List<String>> params = queryStringDecoder.getParameters();
        String path = queryStringDecoder.getPath();
    	List<String> ids = params.get(CONSUMER_ID_KEY);
    	List<String> fileName = params.get(CONFIG_FILE_KEY);
    	       
        if (path.equalsIgnoreCase(START_PRODUCER))
        {        	
        	if ( (null == fileName) || (fileName.isEmpty()))
        	{
        		throw new Exception("Param " + CONFIG_FILE_KEY + " missing !!");
        	}
        	String producerConfigFile = CONFIG_DIR + fileName.get(0);
        	String file = _controller.startProducer(producerConfigFile);
        	
        	return "Response: {\"result\":SUCCESS, \"Message\":" +
        	       " \"Producer started successfully\", \"File\":\"" 
        	       + file + "\"}\r\n";
        	
        } else if (path.equalsIgnoreCase(STOP_PRODUCER)) {
        	
        	_controller.stopGeneration();
        	return "Respose: {\"result\":SUCCESS, \"Message\":" +
        	       "\"Producer stopped successfully\" }\r\n";
        	
        } else if (path.equalsIgnoreCase(SUSPEND_PRODUCER)) {
        	
        	_controller.suspendGeneration();
        	return "Respose: {\"result\":SUCCESS, \"Message\":" +
        	       "\"Producer suspended successfully\" }\r\n";        	
        } else if (path.equalsIgnoreCase(IS_PRODUCER_RUNNING)) {
        	
        	return "Respose: {\"result\":SUCCESS, \"Value\":" + 
        	       _controller.isProducerRunning() + " }\r\n";
        	
        } else if (path.equalsIgnoreCase(GET_PRODUCER_RATE)) {
        	
        	return "Respose: {\"result\":SUCCESS, \"Message\":" +
        	       _controller.getEventProductionRate() + " }\r\n";
        	
        } else if (path.equalsIgnoreCase(START_CONSUMER)) {

        	if ( (null == fileName) || (fileName.isEmpty()))
        	{
        		throw new Exception("Param " + CONFIG_FILE_KEY + " missing !!");
        	}
        	String consumerConfigFile = CONFIG_DIR + fileName.get(0);
        	long id = _controller.startConsumer(consumerConfigFile, false);
        	String file = _controller.getConsumerWriteFile(id);
        	return "Respose: {\"result\":SUCCESS, \"Message\": " +
        	       "\"Consumer started successfully\", \"id\":" 
        	       + id + ", \"file\":\"" + file + "\" }\r\n";
        	
        } else if (path.equalsIgnoreCase(STOP_CONSUMER)) {
        	
        	if ( (null == ids) || (ids.isEmpty()))
        	{
        		throw new Exception("Param id missing !!");
        	}
        	
        	_controller.stopConsumption(Long.parseLong(ids.get(0)),false,0);
        	return "Respose: {\"result\":SUCCESS, \"Message\": "
        	       + "\"Consumer stopped successfully\", \"id\":" 
        	       + ids.get(0) + " }\r\n";
        	
        }  else if (path.equalsIgnoreCase(IS_CONSUMER_RUNNING)) {
        	
        	if ( (null == ids) || (ids.isEmpty()))
        	{
        		throw new Exception("Param id missing !!");
        	}
        	
        	return "Respose: {\"result\":SUCCESS, \"Value\":" + 
        	        !_controller.isConsumerDone(Long.parseLong(ids.get(0))) 
        	        + " }\r\n";
        } else if (path.equalsIgnoreCase(TEST_CONSUMER)) {
        	        	
        	if ( (null == ids) || (ids.isEmpty()))
        	{
        		throw new Exception("Param id missing !!");
        	}
        	
        	_controller.testConsumer(Long.parseLong(ids.get(0)));
        	return "Respose: {\"result\":SUCCESS, \"Message\": " +
        	       "\"Consumer Test passed\", \"id\":" 
 	       		   + ids.get(0) + " }\r\n";
        } else {
        	throw new Exception("Unknown Command:" + path);
        }
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    	
        if (!readingChunks) {
            HttpRequest request = this.request = (HttpRequest) e.getMessage();

            buf.setLength(0);
            /*
            buf.append("WELCOME TO THE WILD WILD WEB SERVER\r\n");
            buf.append("===================================\r\n");

            buf.append("VERSION: " + request.getProtocolVersion() + "\r\n");
            buf.append("HOSTNAME: " + getHost(request, "unknown") + "\r\n");
            buf.append("REQUEST_URI: " + request.getUri() + "\r\n\r\n");

            for (Map.Entry<String, String> h: request.getHeaders()) {
                buf.append("HEADER: " + h.getKey() + " = " + h.getValue() + "\r\n");
            }
            buf.append("\r\n");

            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
            Map<String, List<String>> params = queryStringDecoder.getParameters();
            String path = queryStringDecoder.getPath();
            
            buf.append("PATH: " + path + ":\r\n");
            if (!params.isEmpty()) {
                for (Entry<String, List<String>> p: params.entrySet()) {
                    String key = p.getKey();
                    List<String> vals = p.getValue();
                    for (String val : vals) {
                        buf.append("PARAM: " + key + " = " + val + "\r\n");
                    }
                }
                buf.append("\r\n");
            }
            */
            
            String message = null;
            
            try
            {
              message = processRequest(request);
              buf.append(message);
            } catch (Exception ex) {
              ex.printStackTrace();
              buf.append("Respose: {\"result\":FAIL, \"Message\":\"");
              buf.append(ex.getMessage());
              
              if (ex.getCause() != null)
              {
            	  buf.append("\", \"Cause\":\"");
                  buf.append(ex.getCause());
              }
              buf.append("\" }\r\n");                 
            }
            
            if (request.isChunked()) {
                readingChunks = true;
            } else {
                ChannelBuffer content = request.getContent();
                if (content.readable()) {
                    buf.append("CONTENT: " + content.toString(CharsetUtil.UTF_8) + "\r\n");
                }
                writeResponse(e);
            }
        } else {
            HttpChunk chunk = (HttpChunk) e.getMessage();
            if (chunk.isLast()) {
                readingChunks = false;
                buf.append("END OF CONTENT\r\n");

                HttpChunkTrailer trailer = (HttpChunkTrailer) chunk;
                if (!trailer.getHeaderNames().isEmpty()) {
                    buf.append("\r\n");
                    for (String name: trailer.getHeaderNames()) {
                        for (String value: trailer.getHeaders(name)) {
                            buf.append("TRAILING HEADER: " + name + " = " + value + "\r\n");
                        }
                    }
                    buf.append("\r\n");
                }

                writeResponse(e);
            } else {
                buf.append("CHUNK: " + chunk.getContent().toString(CharsetUtil.UTF_8) + "\r\n");
            }
        }
    }

    private void writeResponse(MessageEvent e) {
        // Decide whether to close the connection or not.
        boolean keepAlive = isKeepAlive(request);

        // Build the response object.
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.setContent(ChannelBuffers.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
        response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

        if (keepAlive) {
            // Add 'Content-Length' header only for a keep-alive connection.
            response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
        }

        // Encode the cookie.
        String cookieString = request.getHeader(COOKIE);
        if (cookieString != null) {
            CookieDecoder cookieDecoder = new CookieDecoder();
            Set<Cookie> cookies = cookieDecoder.decode(cookieString);
            if(!cookies.isEmpty()) {
                // Reset the cookies if necessary.
                CookieEncoder cookieEncoder = new CookieEncoder(true);
                for (Cookie cookie : cookies) {
                    cookieEncoder.addCookie(cookie);
                }
                response.addHeader(SET_COOKIE, cookieEncoder.encode());
            }
        }

        // Write the response.
        ChannelFuture future = e.getChannel().write(response);

        // Close the non-keep-alive connection after the write operation is done.
        if (!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }

    public void justLoop()
    {
      while (true)
      {}
    }
}
