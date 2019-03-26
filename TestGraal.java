import com.walkme.kafka.*;

public class TestGraal {
    public static void main(String[] args) throws Exception{
        try{
            Producer p = new Producer("{\"bootstrap.servers\": \"kafka-broker:9092 \"}");
            p.put("test_topic","msgKey", ",msgValue");  
        } catch(Exception e){
            System.out.print("Error " + e);
        }
        System.out.print("My first program in Java, HelloWorld !!");
    }
}