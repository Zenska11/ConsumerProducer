using Confluent.Kafka;
using Newtonsoft.Json;

public class Programm
{
    static void Main(string[] args)
    {
        var configProducer = new ProducerConfig { BootstrapServers = "localhost:9092" };
        

        using var producer = new ProducerBuilder<Null, string>(configProducer).Build();

       //using var consumer = new ConsumerBuilder<Null, string>(config).Build();

        Thread ConsumerThread = new Thread(
            new ThreadStart(ConsumeFromKafka));
        ConsumerThread.Start();



        try
        {
            string? state;
            while((state = Console.ReadLine()) != null)
            {
                var response = producer.ProduceAsync("weather-topic", 
                    new Message<Null, string> { Value = JsonConvert.SerializeObject(new Weather(state, 70)) });
               // Console.WriteLine(response.Value);
            }


        }
        catch (ProduceException<Null, string> exc)
        {
            Console.WriteLine(exc.Message);
        }

        
    }

    static void ConsumeFromKafka()
    {
        Console.WriteLine("Consumer Loop started");

        var config = new ConsumerConfig
        {
            GroupId = "confirmation-consumer-group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest,

        };
        using var consumer = new ConsumerBuilder<Null, string>(config).Build();

        consumer.Subscribe("confirmation-topic");

        CancellationTokenSource token = new();

        try
        {

            while (true)
            {
                var response = consumer.Consume(token.Token);
                if (response.Message != null)
                {
                    var weather = JsonConvert.DeserializeObject<Weather>(response.Message.Value);
                    Console.WriteLine($"State: {weather.State}, Temp: {weather.Temparature} Grad Antwort");
                }

            }


        }
        catch (ProduceException<Null, string> exc)
        {
            Console.WriteLine(exc.Message);
        }
    }

    public record Weather(string State, int Temparature);


}
