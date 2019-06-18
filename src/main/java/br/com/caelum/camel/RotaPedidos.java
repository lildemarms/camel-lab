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
					log("${id} \n ${body}").
					marshal(getJsonFormater()).
					log("${body}").
					setHeader(Exchange.HTTP_METHOD, constant(org.apache.camel.component.http4.HttpMethods.GET)).
					setHeader(Exchange.HTTP_QUERY, simple("clienteId=${property.clienteId}&pedidoId=${property.pedidoId}&ebookId=${property.ebookId}")).
				to("http4://localhost:8080/webservices/ebook/item");
				
				from("direct:soap").
					routeId("rota-soap").
					setBody(constant("<envelope>teste<envelope>")).
				to("mock:soap");
			}
		});
		
		context.start();
		Thread.sleep(20000);
		context.stop();
	}	
	
	private static XmlJsonDataFormat getJsonFormater() {
		XmlJsonDataFormat xmlJsonFormat = new XmlJsonDataFormat();

		xmlJsonFormat.setEncoding("UTF-8");
		xmlJsonFormat.setForceTopLevelObject(true);
		xmlJsonFormat.setTrimSpaces(true);
		xmlJsonFormat.setRootName("newRoot");
		xmlJsonFormat.setSkipNamespaces(true);
		xmlJsonFormat.setRemoveNamespacePrefixes(true);
		xmlJsonFormat.setExpandableProperties(Arrays.asList("d", "e"));
		
		return xmlJsonFormat;
	}
}
