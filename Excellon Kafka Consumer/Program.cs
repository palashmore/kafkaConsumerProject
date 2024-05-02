// See https://aka.ms/new-console-template for more information

//Console.WriteLine("Hello, World!");
using Confluent.Kafka;

using Newtonsoft.Json;

using System;
using System.Collections.Generic;

class Program
{
    static void Main(string[] args)
    {
         int idCounter = 0;

        // Kafka broker address
        string brokerList = "10.16.4.5:9092";

        // Kafka topic to consume from
        string topicName = "PalashM_ExcellonDataBase";

        // Consumer group ID
        string consumerGroup = "PalashM_Excellon_consumer_group";

        var config = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = consumerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        // List to store consumed data
        List<UserData> userDataList = new List<UserData>();

        // Create a new consumer
        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe(topicName);

            Console.WriteLine("========================================= Waiting for Registration =========================================");

            while (true)
            {
                try
                {
                    // Consume a message
                    var consumeResult = consumer.Consume();

                    UserData userData = JsonConvert.DeserializeObject<UserData>(consumeResult.Message.Value);

                    string newId = $"ES-{++idCounter}";

                    userData.Id = newId;
                    userDataList.Add(userData);

                    //Console.WriteLine($"Consumed message: {consumeResult.Message.Value}");

                    // Display all stored data
                    Console.WriteLine(" ========================================= Welcome to Excellon Employee Details: ========================================= ");
                    Console.WriteLine(" ========================================= XOX ========================================= ");
                    Console.WriteLine("ID\t\tName\t\tMobile\t\tEmail");

                    foreach (var item in userDataList)
                    {
                        Console.WriteLine($"{item.Id,-10}\t{item.Name,-15}\t{item.Mobile,-15}\t{item.Email}");
                    }
                    Console.WriteLine(" ========================================= XOX ========================================= ");

                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occurred: {e.Error.Reason}");
                }
            }
        }
    }
}

// Define a UserData class to match the JSON structure
public class UserData
{
    public string Id { get; set; }
    public string Name { get; set; }
    public string Mobile { get; set; }
    public string Email { get; set; }
}
