using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

using System.Data.SQLite;
using System.Data;

using System.Data.SqlClient;

using Newtonsoft.Json;
using System.Security.Cryptography;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Npgsql;

namespace MessageServer
{


    class Program
    {
        readonly static string sConnStr = new NpgsqlConnectionStringBuilder
        {
            Host = "localhost",
            Port = 5433,
            Database = "postgres",
            Username = "postgres",
            Password = "Faraoni23",
            MaxAutoPrepare = 10,
            AutoPrepareMinUsages = 2
        }.ConnectionString;

        static void AddToDb(dRow row)
        {
            using (var postgConn = new NpgsqlConnection(sConnStr))
            {
                postgConn.Open();

                try
                {
                    //  var reader = row;
                    var postgCommand = new NpgsqlCommand
                    {
                        Connection = postgConn,
                        CommandText = @"INSERT INTO district(id,name,area) VALUES(@id, @name, @area)"
                    };
                    postgCommand.Parameters.AddWithValue("@id", row.district_id);
                    postgCommand.Parameters.AddWithValue("@name", row.district_name);
                    postgCommand.Parameters.AddWithValue("@area", row.district_area);
                    postgCommand.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    // Console.WriteLine(ex.Message);
                }


                try
                {
                    var reader = row;
                    var postgCommand = new NpgsqlCommand();
                    postgCommand.Connection = postgConn;
                    postgCommand.CommandText = @"INSERT INTO town(id,name,population, district_id) VALUES(@id, @name, @population, @district_id)";


                    postgCommand.Parameters.AddWithValue("@id", row.town_id);
                    postgCommand.Parameters.AddWithValue("@name", row.town_name);
                    postgCommand.Parameters.AddWithValue("@population", row.town_population);
                    postgCommand.Parameters.AddWithValue("@district_id", row.district_id);

                    postgCommand.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    // Console.WriteLine(ex.Message);
                }


                {
                    try
                    {
                        var reader = row;
                        var postgCommand = new NpgsqlCommand
                        {
                            Connection = postgConn,
                            CommandText = @"INSERT INTO street(id,name, town_id) VALUES(@id, @name, @town_id)"
                        };

                        postgCommand.Parameters.AddWithValue("@id", row.street_id);
                        postgCommand.Parameters.AddWithValue("@name", row.street_name);
                        postgCommand.Parameters.AddWithValue("@town_id", row.town_id);
                        postgCommand.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        // Console.WriteLine(ex.Message);
                    }
                }

                {
                    try
                    {
                        var reader = row;
                        var postgCommand = new NpgsqlCommand
                        {
                            Connection = postgConn,
                            CommandText = @"INSERT INTO house(id, countfloors, street_id) VALUES(@id, @countfloors, @street_id)"
                        };

                        postgCommand.Parameters.AddWithValue("@id", row.house_id);
                        postgCommand.Parameters.AddWithValue("@countfloors", row.house_countfloors);
                        postgCommand.Parameters.AddWithValue("@street_id", row.street_id);
                        postgCommand.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        //  Console.WriteLine(ex.Message);
                    }
                }

                {
                    try
                    {
                        var reader = row;
                        var postgCommand = new NpgsqlCommand
                        {
                            Connection = postgConn,
                            CommandText = @"INSERT INTO apartments(id, countrooms, area, house_id) VALUES(@id, @countrooms, @area, @house_id)"
                        };
                        postgCommand.Parameters.AddWithValue("@id", row.apart_id);
                        postgCommand.Parameters.AddWithValue("@countrooms", row.apart_countrooms);
                        postgCommand.Parameters.AddWithValue("@area", row.apart_area);
                        postgCommand.Parameters.AddWithValue("@house_id", row.house_id);
                        postgCommand.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        // Console.WriteLine(ex.Message);
                    }
                }



            }
        }




        static int port = 8005; // порт для приема входящих запросов
        static void Main(string[] args)
        {



            // получаем адреса для запуска сокета
            IPEndPoint ipPoint = new IPEndPoint(IPAddress.Parse("0.0.0.0"), port);

            // создаем сокет
            Socket listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                // связываем сокет с локальной точкой, по которой будем принимать данные
                listenSocket.Bind(ipPoint);

                // начинаем прослушивание
                listenSocket.Listen(10);

                Console.WriteLine("Server start, wait connections...");
                int recievedDataSize = 0;
                while (true)
                {

                    Socket handler = listenSocket.Accept();
                    // получаем сообщение
                    StringBuilder builder = new StringBuilder();


                    var DES = new DESCryptoServiceProvider();
                    Console.WriteLine("Recive DES configuration.... ");
                    byte[] keyLength = new byte[4];
                    handler.Receive(keyLength);
                    int sizeIncomingData = BitConverter.ToInt32(keyLength, 0);
                    byte[] bytesKey = new byte[sizeIncomingData];
                    handler.Receive(bytesKey);
                    DES.Key = bytesKey;

                    keyLength = new byte[4];
                    handler.Receive(keyLength);
                    sizeIncomingData = BitConverter.ToInt32(keyLength, 0);
                    bytesKey = new byte[sizeIncomingData];
                    handler.Receive(bytesKey);
                    DES.IV = bytesKey;
                    Console.WriteLine("DES config recived. ");

                    byte[] bytesOfCountRows = new byte[4];
                    handler.Receive(bytesOfCountRows);
                    int countRows = BitConverter.ToInt32(bytesOfCountRows, 0); //размер получаемого файла в байтах
                    Console.WriteLine("Counts rows to recive " + countRows);

                    int countRowsRecive = 0;
                    while (countRowsRecive != countRows)
                    {

                        byte[] bytesSizeOfRows = new byte[4];
                        handler.Receive(bytesSizeOfRows);
                        sizeIncomingData = BitConverter.ToInt32(bytesSizeOfRows, 0); //размер получаемого файла в байтах

                        bytesKey = new byte[sizeIncomingData];
                        handler.Receive(bytesKey);

                        recievedDataSize = bytesKey.Length;
                        using (var decrypt = DES.CreateDecryptor())
                        {


                            dRow dRow;
                            var byteMes = decrypt.TransformFinalBlock(bytesKey, 0, recievedDataSize);
                            var serializeMes = Encoding.UTF8.GetString(byteMes);
                            dRow = (dRow)JsonConvert.DeserializeObject(serializeMes, typeof(dRow));


                            if (dRow != null)
                            {
                                countRowsRecive++;

                                AddToDb(dRow);
                            }


                        }
                    }
                    // закрываем сокет
                    Console.WriteLine("Data load, shutdown connection....");
                    Console.ReadLine();
                    handler.Shutdown(SocketShutdown.Both);

                    handler.Close();
                    Console.WriteLine("Wait other connection....");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }




        }

    }
}
