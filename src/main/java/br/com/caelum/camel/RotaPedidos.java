package br.com.caelum.camel;

import java.util.Arrays;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.xmljson.XmlJsonDataFormat;
import org.apache.camel.impl.DefaultCamelContext;

public class RotaPedidos {

	public static void main(String[] args) throws Exception {

		System.setProperty("org.apache.commons.logging.Log","org.apache.commons.logging.impl.NoOpLog");

		CamelContext context = new DefaultCamelContext();
		context.addRoutes(new RouteBuilder() {
			
			@Override
			public void configure() throws Exception {
				from("file:pedidos?delay=5s&noop=true").
					routeId("rota-pedidos").
				multicast().
					to("direct:soap").
						log("Chamando soap com ${body}").
					to("direct:http");
				
				
				from("direct:http").
					routeId("rota-http").
						setProperty("pedidoId", xpath("/pedido/id/text()")).
						setProperty("clienteId", xpath("/pedido/pagamento/email-titular/text()")).
					split().
			        	xpath("/pedido/itens/item").
					filter().
						xpath("/item/formato[text()='EBOOK']").
					setProperty("ebookId", xpath("/item/livro/codigo/text()")).
					setHeader(Exchange.HTTP_METHOD, constant(org.apache.camel.component.http4.HttpMethods.GET)).
					setHeader(Exchange.HTTP_QUERY, simple("clienteId=${property.clienteId}&pedidoId=${property.pedidoId}&ebookId=${property.ebookId}")).
				to("http4://localhost:8080/webservices/ebook/item");
				
				from("direct:soap").
					routeId("rota-soap").
				to("xslt:pedido-para-soap.xslt").
					log("Resultado do Template: ${body}").
					setHeader(Exchange.CONTENT_TYPE, constant("text/xml")).
				to("http4://localhost:8080/webservices/financeiro");
			}
		});
		
		context.start();
		Thread.sleep(20000);
		context.stop();
	}	
}
