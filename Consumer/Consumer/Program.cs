using Confluent.Kafka;
using Newtonsoft.Json;
using static Confluent.Kafka.ConfigPropertyNames;

public class Programm
{

    static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            GroupId = "offeringsRequest-consumer-group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest,

        };
        using var consumer = new ConsumerBuilder<Null, string>(config).Build();

        

        consumer.Subscribe("offeringsRequest_topic");

        CancellationTokenSource token = new();

        try
        {
            Console.WriteLine("Consumer Loop started");
            while (true)
            {
                var response = consumer.Consume(token.Token);
                if (response.Message != null)
                {
                    var weather = JsonConvert.DeserializeObject<Weather>(response.Message.Value);
                    Console.WriteLine($"State: {weather.State}, Temp: {weather.Temparature} Grad Antwort");
                    Thread ProducerThread = new Thread(
                        new ThreadStart(ProducerToKafka));
                    ProducerThread.Start();
                }
                

            }


        }
        catch (ProduceException<Null, string> exc)
        {
            Console.WriteLine(exc.Message);
        }
    }

    static void ProducerToKafka()
    {
        Console.WriteLine("Produce To Kafka Thread");

        var configProducer = new ProducerConfig { BootstrapServers = "localhost:9092" };
        using var producer = new ProducerBuilder<Null, string>(configProducer).Build();

        try
        {
            var response = producer.ProduceAsync("offeringsResponse_topic",
                new Message<Null, string> { Value = JsonConvert.SerializeObject(new Weather("Antwort", 70)) });
            producer.Flush();
            //Console.WriteLine(response.Value);
        }
        catch (ProduceException<Null, string> exc)
        {
            Console.WriteLine(exc.Message);
        }

    }

    public record Weather(string State, int Temparature);


}