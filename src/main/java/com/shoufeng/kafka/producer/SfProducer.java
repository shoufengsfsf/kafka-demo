package com.shoufeng.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.ExecutionException;

/**
 * @author shoufeng
 */

@Slf4j
@Component
public class SfProducer {

    @Resource
    private KafkaTemplate<Object, Object> kafkaTemplate;

    public ListenableFuture<SendResult<Object, Object>> syncSend(String topic, Object data) throws ExecutionException, InterruptedException {
        // 同步发送消息
        return kafkaTemplate.send(topic, data);
    }

    //注意，如果 Kafka Producer 开启了事务的功能，则所有发送的消息，都必须处于 Kafka 事务之中
    //否则会抛出 " No transaction is in process; possible solutions: run the template operation within the scope of a template.executeInTransaction() operation, start a transaction with @Transactional before invoking the template method, run in a transaction started by a listener container when consuming a record" 异常。
    //所以，如果胖友的业务中，即存在需要事务的情况，也存在不需要事务的情况，需要分别定义两个 KafkaTemplate（Kafka Producer）。
    public String syncSendInTransaction(Integer id, Runnable runner) throws ExecutionException, InterruptedException {
        return kafkaTemplate.executeInTransaction(new KafkaOperations.OperationsCallback<Object, Object, String>() {

            @Override
            public String doInOperations(KafkaOperations<Object, Object> kafkaOperations) {
                try {
                    SendResult<Object, Object> sendResult = kafkaOperations.send("sf_demo_topic_01", "王五").get();
                    log.info("[doInOperations][发送编号：[{}] 发送结果：[{}]]", id, sendResult);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                // 本地业务逻辑... biubiubiu
                runner.run();

                // 返回结果
                return "success";
            }

        });
    }

    @PostConstruct
    public void test() throws ExecutionException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            syncSend("sf_demo_topic_01", "张三" + i);
        }
        System.out.println("开始发送成功: " );
    }

}
