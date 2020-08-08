import com.shoufeng.kafka.KafkaApp;
import com.shoufeng.kafka.producer.SfProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = KafkaApp.class)
public class KafkaAppTest {
    @Autowired
    private SfProducer sfProducer;

    @Resource
    private KafkaTemplate<Object, Object> kafkaTemplate;


    @Test
    public void test() throws ExecutionException, InterruptedException {
        for (int j = 0; j < 5; j++) {
            long start = System.currentTimeMillis();
            System.out.println(sfProducer.syncSend("sf_demo_topic_01", "张三" + j).get());
            System.out.println("用时: " + (System.currentTimeMillis() - start));
        }

    }

    @Test
    public void test02(){
        for (int j = 0; j < 5; j++) {
            kafkaTemplate.send("sf_demo_topic_01",7,"key" + j,"里斯" + j);
        }
    }
}
