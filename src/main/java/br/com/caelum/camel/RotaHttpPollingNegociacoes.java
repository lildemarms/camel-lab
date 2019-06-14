package br.com.caelum.camel;

import java.text.SimpleDateFormat;

import javax.sql.DataSource;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.xstream.XStreamDataFormat;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;
import org.apache.commons.dbcp.BasicDataSource;

import com.thoughtworks.xstream.XStream;

public class RotaHttpPollingNegociacoes {

	private static final String INSERT_QUERY = "insert into negociacao(preco, quantidade, data) values (${property.preco}, ${property.quantidade}, '${property.data}')";

	public static void main(String[] args) throws Exception {

		System.setProperty("org.apache.commons.logging.Log","org.apache.commons.logging.impl.NoOpLog");

		final XStream xstream = new XStream();
		xstream.alias("negociacao", Negociacao.class);
		
		SimpleRegistry registro = new SimpleRegistry();
		registro.put("postgresDataSource", createDataSource());
		
		CamelContext context = new DefaultCamelContext(registro);
		context.addRoutes(new RouteBuilder() {

			@Override
			public void configure() throws Exception {
				from("timer://negociacoes?fixedRate=true&delay=1s&period=360s")
					.to("http4://argentumws-spring.herokuapp.com/negociacoes")
					.convertBodyTo(String.class)
					.unmarshal(new XStreamDataFormat(xstream))
					.split(body())
					.process(configureProcessor())
					.setBody(simple(INSERT_QUERY))
					.log("${body}")
				.delay(1000)
				.to("jdbc:postgresDataSource");
			}			
		});

		context.start();
		Thread.sleep(20000);
		context.stop();
	}
	
	private static Processor configureProcessor() {
		return new Processor() {
			@Override
			public void process(Exchange exchange) throws Exception {
				Negociacao negociacao = exchange.getIn().getBody(Negociacao.class);
				exchange.setProperty("preco", negociacao.getPreco());
				exchange.setProperty("quantidade", negociacao.getQuantidade());

				String data = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
						.format(negociacao.getData().getTime());

				exchange.setProperty("data", data);
			}
		};
	}

	private static DataSource createDataSource() {
		BasicDataSource ds = new BasicDataSource();
		ds.setUsername("postgres");
		ds.setDriverClassName("org.postgresql.Driver");
		ds.setPassword("postgres");
		ds.setUrl("jdbc:postgresql://localhost:5432/camel");
		return ds;
	}
}
