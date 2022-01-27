using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using static SQLite.Program.SQLliteTable;

namespace SQLite
{
    class Program
    {
        public class SQLliteTable
        {
            public struct Doctor
            {
                public string HCPName { get; set; }
                public string Specialty { get; set; }
                public string PhoneNumber { get; set; }
                public string HCOName { get; set; }

                public List<string> WorkPlaces { get; set; }
            }

            public sealed class DoctorMap : ClassMap<Doctor>
            {
                public DoctorMap()
                {
                    AutoMap(CultureInfo.InvariantCulture);
                    Map(m => m.WorkPlaces).Ignore();
                }
            }

            public struct Output
            {
                public string HCPName { get; set; }
                public string Specialty { get; set; }
                public string HCOName { get; set; }
                public string PhoneNumber { get; set; }

            }

            private static String dbFileName;
           
            private static SQLiteConnection m_dbConn = new SQLiteConnection();
            private static SQLiteCommand m_sqlCmd = new SQLiteCommand();

            public SQLliteTable(string databaseFileName)
            {
                dbFileName = databaseFileName;
                
            }
            /// <summary>
            /// Получение данных из файла
            /// </summary>
            /// <param name="path">Путь до файла</param>
            /// <returns> Возвращает структуированный список со всеми данными из файла</returns>
            private List<Doctor> ParseFile(string path)
            {

                var str = File.ReadAllText(path);
                var arrStr = str.Split(new char[] { '\n' });
                List<Doctor> doctors = new List<Doctor>();
                if (path.Contains(".txt"))
                { // Наполняем данными из файла
                    for (int i = 1; i < arrStr.Count(); i++)
                    {
                        var item = arrStr[i];
                        var arrItem = item.Split(new char[] { '	' });
                        doctors.Add(new Doctor() { HCPName = arrItem[0], Specialty = arrItem[1], PhoneNumber = arrItem[2], HCOName = arrItem[3] });
                    }
                }
                else if (path.Contains(".csv"))
                {
                    StreamReader reader = new StreamReader(path, Encoding.GetEncoding(1251));
                    var csv = new CsvReader(reader, CultureInfo.CurrentCulture);

                    doctors = csv.GetRecords<Doctor>().ToList();
                }
                var listDoctors = new List<Doctor>();
                // Поиск на наличие нескольких мест работы, условие (Несколько одинаковых экземпляров=нескольким местам работы
                foreach (var item in doctors)
                {
                    var list = new List<string>();
                    var l = doctors.Where(p => p.HCPName == item.HCPName).ToList();
                    foreach (var pl in l)
                        list.Add(pl.HCOName);
                    listDoctors.Add(new Doctor() { HCPName = item.HCPName, PhoneNumber = item.PhoneNumber, Specialty = item.Specialty, WorkPlaces = list });

                }
                //Чистим список от дублей
                var ClearList = listDoctors;
                for (int i = 0; i < listDoctors.Count; i++)
                {
                    var item = listDoctors[i];
                    var l = listDoctors.Where(p => p.HCPName == item.HCPName).ToList();
                    if (l.Count > 1) ClearList.Remove(item);
                }




                return ClearList;
            }
            /// <summary>
            /// Зашифровывает строку
            /// </summary>
            /// <param name="rawData">Исходная строка</param>
            /// <returns>Возвращает захешированную строку</returns>
            private string EncryptSha256Hash(string rawData)
            {
                // Create a SHA256   
                using (SHA256 sha256Hash = SHA256.Create())
                {
                    // ComputeHash - returns byte array  
                    byte[] bytes = sha256Hash.ComputeHash(Encoding.UTF8.GetBytes(rawData));

                    // Convert byte array to a string   
                    StringBuilder builder = new StringBuilder();
                    for (int i = 0; i < bytes.Length; i++)
                    {
                        builder.Append(bytes[i].ToString("x2"));
                    }
                    return builder.ToString();
                }
            }
            /// <summary>
            /// Создает подключение к базе данных
            /// </summary>
            public void Connect()
            {
                if (!File.Exists(dbFileName))
                    SQLiteConnection.CreateFile(dbFileName);
                try
                {
                    m_dbConn = new SQLiteConnection("Data Source=" + dbFileName + ";Version=3;");
                    m_dbConn.Open();
                    m_sqlCmd.Connection = m_dbConn;
                }
                catch (SQLiteException ex)
                {
                    Console.WriteLine("Disconnected");
                    Console.WriteLine("Error: " + ex.Message);
                }

            }
            /// <summary>
            /// Разрывает подключение с базой данных
            /// </summary>
            public void Disconnect()
            {
                m_dbConn.Close();
            }

            /// <summary>
            /// Метод создает таблицу в базе данных
            /// </summary>
            /// <param name="tableName">Имя таблицы</param>
            /// <param name="Fields">Поля таблицы</param>
            public void Create(string tableName, string Fields)
            {
                m_sqlCmd.CommandText = $"CREATE TABLE IF NOT EXISTS {tableName} ({Fields})";
                m_sqlCmd.ExecuteNonQuery();
            }
            /// <summary>
            /// Добавляет врачей в базу данных из структуированного списка
            /// </summary>
            public void AddDoctors(string filepath)
            {
                m_sqlCmd.CommandText = "Select count(*) from Doctors";
                int count = Convert.ToInt32(m_sqlCmd.ExecuteScalar());

                List<Doctor> data = ParseFile(filepath);
                foreach (var item in data)
                {
                    string sql = $"INSERT INTO Doctors ('HCPName','Specialty','PhoneNumber') Values ('{item.HCPName}','{item.Specialty}','{EncryptSha256Hash(item.PhoneNumber).Trim()}') returning id";
                    m_sqlCmd.CommandText = sql;
                    string id = m_sqlCmd.ExecuteScalar().ToString();
                    foreach (var work in item.WorkPlaces)
                    {
                        m_sqlCmd.CommandText = $"INSERT INTO Workplace ('DoctorID','HCOName') Values ({id},'{work}')";
                        m_sqlCmd.ExecuteNonQuery();
                    }

                }
                Console.WriteLine("Done!");






            }
            /// <summary>
            /// Добавление доктора в базу
            /// </summary>
            /// <param name="item"></param>
            public void AddDoctor(Doctor item)
            {
                string sql = $"INSERT INTO Doctors ('HCPName','Specialty','PhoneNumber') Values ('{item.HCPName}','{item.Specialty}','{EncryptSha256Hash(item.PhoneNumber).Trim()}') returning id";
                m_sqlCmd.CommandText = sql;
                string id = m_sqlCmd.ExecuteScalar().ToString();
                foreach (var work in item.WorkPlaces)
                {
                    m_sqlCmd.CommandText = $"INSERT INTO Workplace ('DoctorID','HCOName') Values ({id},'{work}')";
                    m_sqlCmd.ExecuteNonQuery();
                }


            }
            /// <summary>
            /// Поиск дублей по хешу номера телефона
            /// </summary>
            public void GetDuplicates()
            {
                DataTable dTable = new DataTable();
                string sql = "SELECT * from Doctors where PhoneNumber in( SELECT PhoneNumber FROM Doctors GROUP BY PhoneNumber HAVING COUNT(PhoneNumber) > 1)";// sql поиска сотрудников по хешу номера телефона
                SQLiteDataAdapter adapter = new SQLiteDataAdapter(sql, m_dbConn);
                adapter.Fill(dTable);
                if (dTable.Rows.Count > 0)
                {
                    Console.WriteLine("Найдены дубли по хешу номера у следующих людей: ");
                    foreach (DataRow row in dTable.Rows)
                        Console.WriteLine($"ФИО:{row[1].ToString()}");
                }
                else Console.WriteLine("Дубли не найдены!");


            }
            /// <summary>
            /// Обновление места работы
            /// </summary>
            /// <param name="HCPName"></param>
            public void UpdateWorkPlace(string HCPName)
            {
                DataTable dTable = new DataTable();
                string sql = $"SELECT HCOName,id from Workplace where DoctorID=(Select id from Doctors where HCPName='{HCPName}')";// sql поиска сотрудников по хешу номера телефона
                SQLiteDataAdapter adapter = new SQLiteDataAdapter(sql, m_dbConn);
                adapter.Fill(dTable);
                if (dTable.Rows.Count > 0)
                {
                    int count = 1;
                    foreach (DataRow row in dTable.Rows)
                    {
                        Console.WriteLine($"{count}:{row[0].ToString()}");
                        count++;
                    }
                    Console.Write("Введите номер места работы для изменения:");
                    var id = Convert.ToInt32(dTable.Rows[Convert.ToInt32(Console.ReadLine()) - 1][1]);
                    Console.Write("Введите место работы:");
                    var place = Console.ReadLine();
                    m_sqlCmd.CommandText = $"UPDATE Workplace SET HCOName = '{place}' where id={id}";
                    m_sqlCmd.ExecuteNonQuery();
                    Console.WriteLine("Место работы обновлено");
                }
                else Console.WriteLine("Доктор не найден");


            }
            /// <summary>
            /// Выгрузка из базы в csv файл
            /// </summary>
            public void Download()
            {
                DataTable dTable = new DataTable();
                string sql = $"SELECT HCPName,Specialty,HCOName from Doctors JOIN Workplace on Workplace.DoctorID=Doctors.Id order by Specialty,HCOName ASC ";// sql поиска сотрудников по хешу номера телефона
                SQLiteDataAdapter adapter = new SQLiteDataAdapter(sql, m_dbConn);
                adapter.Fill(dTable);
                if (dTable.Rows.Count > 0)
                {
                    List<Output> data = new List<Output>();

                    foreach (DataRow row in dTable.Rows)
                        data.Add(new Output() { HCPName = row[0].ToString(), Specialty = row[1].ToString(), HCOName = row[2].ToString() });

                    using (var writer = new StreamWriter(@"file.csv", false, Encoding.UTF8))
                    using (var csvWriter = new CsvWriter(writer, CultureInfo.CurrentCulture))
                    {

                        csvWriter.WriteHeader<Output>();
                        csvWriter.NextRecord();
                        csvWriter.WriteRecords(data);
                    }
                    Console.WriteLine("Done!");

                }
                else Console.WriteLine("Записей не найдено!");



            }


        }








        static void Main(string[] args)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);





            bool isRunning = true;
            SQLliteTable table = new SQLliteTable("fake_data");
            table.Connect();
            table.Create("Doctors", "id INTEGER PRIMARY KEY AUTOINCREMENT, HCPName TEXT, Specialty TEXT, PhoneNumber Text");
            table.Create("Workplace", "id INTEGER PRIMARY KEY AUTOINCREMENT, DoctorID INTEGER, HCOName TEXT");
            while (isRunning)
            {
                try
                {
                    string newLine = Environment.NewLine;
                    Console.WriteLine($@"{newLine} 1.Загрузить список врачей {newLine} 2.Искать совпадения в бд по хэшу номера {newLine} 3.Добавить нового врача {newLine} 4.Обновить место работы врача {newLine} 5.Выгрузить список врачей в csv файл {newLine} 6.Завершить работу {newLine} Введите номер задачи:");
                    string taskId = Console.ReadLine();
                    if (taskId == "1")
                    {
                        Console.Write("Укажите путь к файлу: ");
                        string path = Console.ReadLine();
                        table.AddDoctors(path);
                    }
                    else if (taskId == "2")
                        table.GetDuplicates();
                    else if (taskId == "3")
                    {
                        string fio = string.Empty;
                        string phonenumber = string.Empty;
                        string direction = string.Empty;
                        string hospital = string.Empty;
                        Console.Write("Введите ФИО:");
                        fio = Console.ReadLine();
                        Console.Write("Введите направление работы:");
                        direction = Console.ReadLine();
                        Console.Write("Введите номер телефона:");
                        phonenumber = Console.ReadLine();
                        Console.Write("Введите количество мест работы :");
                        int howMany = Convert.ToInt32(Console.ReadLine());
                        List<string> places = new List<string>();
                        for (int i = 1; i <= howMany; i++)
                        {
                            Console.WriteLine($"{i} место работы: ");
                            places.Add(Console.ReadLine());
                        }
                        table.AddDoctor(new Doctor() { HCPName = fio, PhoneNumber = phonenumber, Specialty = direction, WorkPlaces = places });
                        Console.WriteLine("Работник добавлен!");
                    }

                    else if (taskId == "4")
                    {
                        Console.Write("Введите фио:");
                        table.UpdateWorkPlace(Console.ReadLine());
                    }

                    else if (taskId == "5")
                        table.Download();

                    else if (taskId == "6") isRunning = false;

                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            table.Disconnect();


        }
    }
}

