const {Worker} = require('worker_threads');

function JavaToJSNotifier() {
    this.queue = new java.util.concurrent.LinkedBlockingDeque();
    this.worker = new Worker(`
        const { workerData, parentPort } = require('worker_threads');
        console.log("Yo")
        while (true) {
          // block the worker waiting for the next notification from Java
          parentPort.postMessage('Worker waiting');
          var data = workerData.queue.take();
          // notify the main event loop that we got new data 
          parentPort.postMessage(data);
        }`,
        { eval: true, workerData: { queue: this.queue }, stdout: true, stderr: true });
}

const Producer = Java.type('com.walkme.kafka.Producer');
const config = {
    "bootstrap.servers": (process.env.VM === 'graalvm') ?'kafka-broker:9093' : 'localhost:9092'
}
//const mProducer = new Producer(JSON.stringify(config));
//mProducer.put("test_topic", "msgKey", "msgKey");

const Consumer = Java.type('com.walkme.kafka.Consumer');
config.topic = "test_topic";
config['group.id'] = 'Test_Group'

const asyncJavaEvents = new JavaToJSNotifier();
asyncJavaEvents.worker.on('message', (n) => {
    console.log(`Got new data from Java! ${n}`);
});

const mConsumer = new Consumer(asyncJavaEvents.queue, JSON.stringify(config));
mConsumer.start();

