package com.baidu;


import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RabbitListener(queues = AmqpConfig.FOO_QUEUE)
public class Listener {

    private static final Logger LOGGER = LoggerFactory.getLogger(Listener.class);
    @Autowired
    private AmqpConfig amqpConfig;

    /** 设置交换机类型  */
    @Bean
    public TopicExchange defaultExchange() {
        /**
         * DirectExchange:按照routingkey分发到指定队列
         * TopicExchange:多关键字匹配
         * FanoutExchange: 将消息分发到所有的绑定队列，无routingkey的概念
         * HeadersExchange ：通过添加属性key-value匹配
         */
        return new TopicExchange(AmqpConfig.FOO_EXCHANGE);
    }

    @Bean
    public Queue fooQueue() {
        return new Queue(AmqpConfig.FOO_QUEUE);
    }

    @Bean
    public Binding binding() {
        /** 将队列绑定到交换机 */
        return BindingBuilder.bind(fooQueue()).to(defaultExchange()).with(AmqpConfig.FOO_ROUTINGKEYTWO);
    }

//    @RabbitHandler
//    public void process(@Payload String foo) {
//        LOGGER.info("Listener: " + foo);
//    }


//    或者使用下面的代码来代替@RabbitHandler注解的process方法
    @Bean
    public SimpleMessageListenerContainer messageContainer() {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(amqpConfig.connectionFactory());
        container.setQueues(fooQueue());
        container.setExposeListenerChannel(true);
        container.setMaxConcurrentConsumers(1);
        container.setConcurrentConsumers(1);//setConcurrentConsumers 方法就是用来指定并发消费者的数量
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL); //设置确认模式手工确认
        container.setMessageListener(new ChannelAwareMessageListener() {
            @Override
            public void onMessage(Message message, Channel channel) throws Exception {
                byte[] body = message.getBody();

                //消费消息
                String s = new String(body);

                LOGGER.info("Listener onMessage : " + s);
                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false); //确认消息成功消费
            }
        });
        return container;
    }
}
