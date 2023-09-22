using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using TitaniumAS.Opc.Client.Common;
using TitaniumAS.Opc.Client.Da;

namespace OPCDA
{
    class Program
    {
        static Uri url = UrlBuilder.Build("OSISoft.OPCDA2.DA.1", "172.30.4.2");
        static string MQTT_HOST = "itrend.dev.itthirit.io";
        static string ClientId = "C#Program";
        static string Username = "itthirit";
        static string Password = "P@ssw0rd";
        static string[] PrefixTopic = new string[10];
        static DateTime dt = DateTime.Now;
        static System.Timers.Timer readTimer;
        static System.Timers.Timer publishTimer;
        static OpcDaItemValue[] values_WT, values_PQM, values_MAIN, values_SUB, values_SINV, values_W_PQM, values_W_WTG, values_W_PM = null;
        static OpcDaGroup group_WT, group_PQM, group_MAIN, group_SUB, group_SINV, group_W_PQM, group_W_WTG, group_W_PM;
        static List<OpcDaItemDefinition> SINV = OpcTags.ItemId_SINV();
        static List<OpcDaItemDefinition> PQM = OpcTags.ItemId_PQM();
        static List<OpcDaItemDefinition> WT = OpcTags.ItemId_WT();
        static List<OpcDaItemDefinition> MAIN = OpcTags.ItemId_MAIN();
        static List<OpcDaItemDefinition> SUB = OpcTags.ItemId_SUB();
        static List<OpcDaItemDefinition> W_PQM = OpcTags.ItemId_W_PQM();
        static List<OpcDaItemDefinition> W_WTG = OpcTags.ItemId_W_WTG();
        static List<OpcDaItemDefinition> W_PM = OpcTags.ItemId_W_PM();
        static void Main(string[] args)
        {
            TitaniumAS.Opc.Client.Bootstrap.Initialize();
            using (var server = new OpcDaServer(url))
            {
                try
                {
                    server.Connect();
                    group_SINV = server.AddGroup("SINV");
                    group_PQM = server.AddGroup("PQM");
                    group_WT = server.AddGroup("WT");
                    group_MAIN = server.AddGroup("MAIN");
                    group_SUB = server.AddGroup("SUB");
                    group_W_PQM = server.AddGroup("W_PQM");
                    group_W_WTG = server.AddGroup("W_WTG");
                    group_W_PM = server.AddGroup("W_PM");

                    group_SINV.IsActive = true;
                    group_PQM.IsActive = true;
                    group_WT.IsActive = true;
                    group_MAIN.IsActive = true;
                    group_SUB.IsActive = true;
                    group_W_PQM.IsActive = true;
                    group_W_WTG.IsActive = true;
                    group_W_PM.IsActive = true;

                    OpcDaItemResult[] results_SINV = group_SINV.AddItems(SINV.ToArray());
                    OpcDaItemResult[] results_PQM = group_PQM.AddItems(PQM.ToArray());
                    OpcDaItemResult[] results_WT = group_WT.AddItems(WT.ToArray());
                    OpcDaItemResult[] results_MAIN = group_MAIN.AddItems(MAIN.ToArray());
                    OpcDaItemResult[] results_SUB = group_SUB.AddItems(SUB.ToArray());
                    OpcDaItemResult[] results_W_PQM = group_W_PQM.AddItems(W_PQM.ToArray());
                    OpcDaItemResult[] results_W_WTG = group_W_WTG.AddItems(W_WTG.ToArray());
                    OpcDaItemResult[] results_W_PM = group_W_PM.AddItems(W_PM.ToArray());

                    PrintErrors(results_SINV, "SINV");
                    PrintErrors(results_PQM, "PQM");
                    PrintErrors(results_WT, "WT");
                    PrintErrors(results_MAIN, "MAIN");
                    PrintErrors(results_SUB, "SUB");
                    PrintErrors(results_W_PQM, "Wind PQM");
                    PrintErrors(results_W_WTG, "Wind WTG");
                    PrintErrors(results_W_PM, "Wind METER");

                    // Set up a timer to read OPC DA values every 5 seconds
                    readTimer = new System.Timers.Timer(30000);
                    readTimer.Elapsed += ReadTimerElapsed;
                    readTimer.Start();

                    // Set up a timer to publish MQTT values every 10 seconds
                    publishTimer = new System.Timers.Timer(60000);
                    publishTimer.Elapsed += PublishTimerElapsed;
                    publishTimer.Start();
                }
                catch (Exception ex) { Console.WriteLine(ex.Message); }
                while (true)
                {

                    // Run forever
                }
            }
        }

        private static void ReadTimerElapsed(object sender, ElapsedEventArgs e)
        {
            // อ่านข้อมูลจากกลุ่มของอุปกรณ์ที่ต้องการด้วย OPC DA
            values_SINV = group_SINV.Read(group_SINV.Items, OpcDaDataSource.Device);
            values_PQM = group_PQM.Read(group_PQM.Items, OpcDaDataSource.Device);
            values_WT = group_WT.Read(group_WT.Items, OpcDaDataSource.Device);
            values_MAIN = group_MAIN.Read(group_MAIN.Items, OpcDaDataSource.Device);
            values_SUB = group_SUB.Read(group_SUB.Items, OpcDaDataSource.Device);
            values_W_PQM = group_SUB.Read(group_W_PQM.Items, OpcDaDataSource.Device);
            values_W_WTG = group_SUB.Read(group_W_PQM.Items, OpcDaDataSource.Device);
            values_W_PM = group_SUB.Read(group_W_PQM.Items, OpcDaDataSource.Device);

            // ตรวจสอบและแสดงข้อความบนคอนโซลเซิร์ฟเวอร์ (Console) เมื่อมีการอ่านข้อมูล
            // โดยใช้เงื่อนไขตรวจสอบ null ก่อน
            PrintReadStatus(values_SINV, "INVERTER");
            PrintReadStatus(values_PQM, "PQM");
            PrintReadStatus(values_WT, "ENVIROMENTAL");
            PrintReadStatus(values_MAIN, "MAIN");
            PrintReadStatus(values_SUB, "SUB");
            PrintReadStatus(values_W_PQM, "Wind PQM");
            PrintReadStatus(values_W_WTG, "Wind WTG");
            PrintReadStatus(values_W_PM, "Wind METER");
        }

        private static void PrintReadStatus(object values, string deviceName)
        {
            // ตรวจสอบว่าข้อมูลไม่เป็น null แล้วแสดงข้อความบนคอนโซลเซิร์ฟเวอร์
            if (values != null)
            {
                string timestamp = DateTime.Now.ToString("yy-MM-dd hh:mm:ss");
                Console.WriteLine($"Read OPC DA : {deviceName} DT:{timestamp}");
            }
        }

        private static async void PublishTimerElapsed(object sender, ElapsedEventArgs e)
        {
            // กำหนดหัวข้อที่ใช้ในการ publish ข้อมูลไปยัง MQTT
            string[] PrefixTopic =
            {
                "BGRIMM/SOLARFARM/1/INVERTER/",
                "BGRIMM/SOLARFARM/1/PVS/",
                "BGRIMM/SOLARFARM/1/ENVIROMENTAL/",
                "BGRIMM/SOLARFARM/1/PQM/",
                "BGRIMM/SOLARFARM/1/MAIN/",
                "BGRIMM/SOLARFARM/1/SUB/",
                "BGRIMM/WIND/1/PQM/",
                "BGRIMM/WIND/1/WTG/",
                "BGRIMM/WIND/1/METER/",
            };

            // กำหนดข้อมูลที่ต้องการ publish สำหรับแต่ละหัวข้อ
            OpcDaItemValue[][] valuesToPublish = new OpcDaItemValue[][]
            {
                values_SINV,   // For INVERTER
                values_SINV,   // For PVS
                values_WT,     // For ENVIROMENTAL
                values_PQM,    // For PQM
                values_MAIN,   // For SWITCHGEAR
                values_SUB,    // For Ring Main Unit Incoming, Ring Main Unit Outgoing, Transformer
                values_W_PQM,  // For Wind Turbine PQM
                values_W_WTG,  // For Wind Turbine
                values_W_PM,   // For Wind Turbine
            };

            // วนลูปเพื่อ publish ข้อมูลสำหรับแต่ละหัวข้อ
            for (int i = 0; i < PrefixTopic.Length; i++)
            {
                if (valuesToPublish[i] != null)
                {
                    try
                    {
                        // เรียกใช้งานฟังก์ชัน Publish_MQTT ด้วยการใช้ Task.Run เพื่อทำให้เป็นการทำงานแบบ asynchronous
                        await Task.Run(() => Publish_MQTT(i, PrefixTopic[i], valuesToPublish[i]));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                }
            }
        }


        static async Task Publish_MQTT(int deviceTypeId, string topicPrefix, OpcDaItemValue[] values)
        {
            try
            {
                DateTime dt = DateTime.Now;
                int MaxDeviceId = 171; //DeviceId +1 
                var mqttFactory = new MqttFactory();
                Dictionary<string, string>[] opcdaItems = Enumerable.Range(0, MaxDeviceId)
                .Select(_ => new Dictionary<string, string>()) // _ เพื่อระบุว่าเราไม่ใช้ค่านี
                .ToArray();
                string solarItem = "";
                string windItem = "";
                string jsonPayload = "";
                string[] topic = new string[MaxDeviceId];
                int deviceId;
                using (var mqttClient = mqttFactory.CreateMqttClient())
                {
                    var mqttClientOptions = new MqttClientOptionsBuilder()
                        .WithTcpServer(MQTT_HOST)
                        .WithClientId(ClientId)
                        .WithCredentials(Username, Password)
                        .Build();
                    await mqttClient.ConnectAsync(mqttClientOptions, CancellationToken.None);

                    for (int i = 0; i < values.Count(); i++)
                    {
                        if (deviceTypeId < 6)
                        {
                            solarItem = GetTextAfterLastDot(values[i].Item.ItemId);
                        }
                        else
                        {
                            windItem = GetTextAfterLastUnderscore(values[i].Item.ItemId);
                        }


                        string prefixSINV1 = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.SINV";
                        string prefixSINV2 = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub2.SINV";
                        int startIndex = values[i].Item.ItemId.IndexOf("SINV") + "SINV".Length;
                        string subNumber = values[i].Item.ItemId.Substring(startIndex, 2);
                        switch (deviceTypeId)
                        {
                            case 0:
                                if (values[i].Item.ItemId.StartsWith(prefixSINV1))
                                {
                                    deviceId = int.Parse(subNumber);
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][solarItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixSINV2))
                                {
                                    deviceId = int.Parse(subNumber) + 38;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][solarItem] = values[i].Value.ToString();
                                }
                                break;
                            case 1:
                                if (values[i].Item.ItemId.StartsWith(prefixSINV1))
                                {
                                    deviceId = int.Parse(subNumber) + 76;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][solarItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixSINV2))
                                {
                                    deviceId = int.Parse(subNumber) + 114;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][solarItem] = values[i].Value.ToString();
                                }
                                break;
                            case 2:
                                string prefixWT1 = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.WT.";
                                if (values[i].Item.ItemId.StartsWith(prefixWT1))
                                {
                                    deviceId = 153;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][solarItem] = values[i].Value.ToString();
                                }
                                break;
                            case 3:
                                string prefixPQM1 = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.";
                                if (values[i].Item.ItemId.StartsWith(prefixPQM1))
                                {
                                    deviceId = 154;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][solarItem] = values[i].Value.ToString();
                                }
                                break;
                            case 4: //MAIN
                                string prefixSWG1 = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.SWG.";
                                if (values[i].Item.ItemId.StartsWith(prefixSWG1) ||
                                    values[i].Item.ItemId == "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.FireAlarm" ||
                                    values[i].Item.ItemId == "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.BattLossACInput" ||
                                    values[i].Item.ItemId == "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.BattChargeFail" ||
                                    values[i].Item.ItemId == "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.BattLowDC" ||
                                    values[i].Item.ItemId == "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.BattHighDC")
                                {
                                    deviceId = 155;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][solarItem] = values[i].Value.ToString();
                                }
                                break;
                            case 5: //SUB
                                string prefixRMUIn1 = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUIn.";
                                string prefixRMUOut1 = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut1.";
                                string prefixRMUOut2 = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut2.";
                                string prefixTR1 = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.TR1.";
                                string prefixTR2 = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub2.TR2.";
                                if (values[i].Item.ItemId.StartsWith(prefixRMUIn1))
                                {
                                    deviceId = 156;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][solarItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixRMUOut1))
                                {
                                    deviceId = 157;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][solarItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixRMUOut2))
                                {
                                    deviceId = 158;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][solarItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixTR1))
                                {
                                    deviceId = 159;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][solarItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixTR2))
                                {
                                    deviceId = 160;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][solarItem] = values[i].Value.ToString();
                                }
                                break;
                            case 6:
                                string prefixWPQM1 = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_PQM1_";
                                string prefixWPQM2 = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_PQM2_";
                                if (values[i].Item.ItemId.StartsWith(prefixWPQM1))
                                {
                                    deviceId = 161;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][windItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixWPQM2))
                                {
                                    deviceId = 162;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][windItem] = values[i].Value.ToString();
                                }
                                break;
                            case 7:
                                string prefixWWTG1 = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG01_";
                                string prefixWWTG2 = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG02_";
                                string prefixWWTG3 = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG03_";
                                string prefixWWTG4 = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG04_";
                                if (values[i].Item.ItemId.StartsWith(prefixWWTG1))
                                {
                                    deviceId = 163;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][windItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixWWTG2))
                                {
                                    deviceId = 164;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][windItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixWWTG3))
                                {
                                    deviceId = 165;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][windItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixWWTG4))
                                {
                                    deviceId = 166;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][windItem] = values[i].Value.ToString();
                                }
                                break;
                            case 8:
                                string prefixWOUT1 = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_OUT1_";
                                string prefixWOUT2 = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_OUT2_";
                                string prefixWCAP1 = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_CAP1_";
                                string prefixWCAP2 = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_CAP2_";
                                if (values[i].Item.ItemId.StartsWith(prefixWOUT1))
                                {
                                    deviceId = 167;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][windItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixWOUT2))
                                {
                                    deviceId = 168;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][windItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixWCAP1))
                                {
                                    deviceId = 169;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][windItem] = values[i].Value.ToString();
                                }
                                if (values[i].Item.ItemId.StartsWith(prefixWCAP2))
                                {
                                    deviceId = 170;
                                    topic[deviceId] = topicPrefix + deviceId.ToString();
                                    opcdaItems[deviceId][windItem] = values[i].Value.ToString();
                                }
                                break;
                        }
                    }

                    for (int i = 0; i < MaxDeviceId; i++)
                    {
                        var mqttItems = new Dictionary<string, string>();
                        if (opcdaItems[i] != null && topic[i] != null)
                        {
                            switch (deviceTypeId)
                            {
                                case 0: //SINV
                                    mqttItems["ID"] = i.ToString();
                                    mqttItems["Date"] = dt.ToString("yyyy-MM-dd");
                                    mqttItems["Time"] = dt.ToString("HH:mm:ss"); ;
                                    mqttItems["AC_Voltage_L1"] = opcdaItems[i]["VoltageA"];
                                    mqttItems["AC_Voltage_L2"] = opcdaItems[i]["VoltageB"];
                                    mqttItems["AC_Voltage_L3"] = opcdaItems[i]["VoltageC"];
                                    mqttItems["AC_Voltage_L12"] = "";
                                    mqttItems["AC_Voltage_L23"] = "";
                                    mqttItems["AC_Voltage_L31"] = "";
                                    mqttItems["AC_Current_L1"] = opcdaItems[i]["CurrentA"];
                                    mqttItems["AC_Current_L2"] = opcdaItems[i]["CurrentB"];
                                    mqttItems["AC_Current_L3"] = opcdaItems[i]["CurrentC"];
                                    mqttItems["AC_Current_Total"] = "";
                                    mqttItems["AC_Power_Active_Total"] = opcdaItems[i]["TotalActivePower"];
                                    mqttItems["AC_Power_Apparent_Total"] = opcdaItems[i]["TotalAppearentPower"];
                                    mqttItems["AC_Power_Reactive_Total"] = opcdaItems[i]["TotalReactivePower"];
                                    mqttItems["Peak_Active_Power"] = "";
                                    mqttItems["Power_Factor"] = opcdaItems[i]["PowerFactor"];
                                    mqttItems["Frequency"] = opcdaItems[i]["GridFrequency"];
                                    mqttItems["Efficency"] = "";
                                    mqttItems["Internal_Temp"] = opcdaItems[i]["Internal_temperature"];
                                    mqttItems["Insulation_Resistance"] = "";
                                    mqttItems["Energy_Active_Total"] = opcdaItems[i]["TotalPowerYield"];
                                    mqttItems["Energy_Active_Daily"] = opcdaItems[i]["DailyPowerYield"];
                                    mqttItems["DC_Voltage_Total"] = opcdaItems[i]["DCVoltage"];
                                    mqttItems["DC_Current_Total"] = opcdaItems[i]["DCCurrent"];
                                    mqttItems["DC_Power_Total"] = opcdaItems[i]["TotalDCPower"];
                                    mqttItems["Operation_State_1"] = "";
                                    mqttItems["Operation_State_2"] = "";
                                    mqttItems["Operation_State_3"] = "";
                                    mqttItems["Alarm_Code_1"] = "";
                                    mqttItems["Alarm_Code_2"] = "";
                                    mqttItems["Alarm_Code_3"] = "";
                                    mqttItems["Device_Status"] = "";
                                    break;
                                case 1: //PSV
                                    mqttItems["ID"] = i.ToString();
                                    mqttItems["Date"] = dt.ToString("yyyy-MM-dd");
                                    mqttItems["Time"] = dt.ToString("HH:mm:ss");
                                    mqttItems["Voltage_String1"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String1"] = opcdaItems[i]["StringCurrent1"];
                                    mqttItems["Power_String1"] = (double.Parse(opcdaItems[i]["StringCurrent1"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String2"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String2"] = opcdaItems[i]["StringCurrent2"];
                                    mqttItems["Power_String2"] = (double.Parse(opcdaItems[i]["StringCurrent2"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String3"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String3"] = opcdaItems[i]["StringCurrent3"];
                                    mqttItems["Power_String3"] = (double.Parse(opcdaItems[i]["StringCurrent3"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String4"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String4"] = opcdaItems[i]["StringCurrent4"];
                                    mqttItems["Power_String4"] = (double.Parse(opcdaItems[i]["StringCurrent4"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String5"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String5"] = opcdaItems[i]["StringCurrent5"];
                                    mqttItems["Power_String5"] = (double.Parse(opcdaItems[i]["StringCurrent5"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String6"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String6"] = opcdaItems[i]["StringCurrent6"];
                                    mqttItems["Power_String6"] = (double.Parse(opcdaItems[i]["StringCurrent6"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String7"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String7"] = opcdaItems[i]["StringCurrent7"];
                                    mqttItems["Power_String7"] = (double.Parse(opcdaItems[i]["StringCurrent7"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String8"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String8"] = opcdaItems[i]["StringCurrent8"];
                                    mqttItems["Power_String8"] = (double.Parse(opcdaItems[i]["StringCurrent8"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String9"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String9"] = opcdaItems[i]["StringCurrent9"];
                                    mqttItems["Power_String9"] = (double.Parse(opcdaItems[i]["StringCurrent9"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String10"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String10"] = opcdaItems[i]["StringCurrent10"];
                                    mqttItems["Power_String10"] = (double.Parse(opcdaItems[i]["StringCurrent10"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String11"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String11"] = opcdaItems[i]["StringCurrent11"];
                                    mqttItems["Power_String11"] = (double.Parse(opcdaItems[i]["StringCurrent11"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String12"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String12"] = opcdaItems[i]["StringCurrent12"];
                                    mqttItems["Power_String12"] = (double.Parse(opcdaItems[i]["StringCurrent12"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String13"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String13"] = opcdaItems[i]["StringCurrent13"];
                                    mqttItems["Power_String13"] = (double.Parse(opcdaItems[i]["StringCurrent13"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Voltage_String14"] = opcdaItems[i]["BusVoltage"];
                                    mqttItems["Current_String14"] = opcdaItems[i]["StringCurrent14"];
                                    mqttItems["Power_String14"] = (double.Parse(opcdaItems[i]["StringCurrent14"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    mqttItems["Current_Total"] = opcdaItems[i]["DCCurrent"];
                                    mqttItems["Power_Total"] = (double.Parse(opcdaItems[i]["DCCurrent"]) * double.Parse(opcdaItems[i]["BusVoltage"])).ToString();
                                    break;
                                case 2: //WT
                                    mqttItems["ID"] = i.ToString();
                                    mqttItems["Date"] = dt.ToString("yyyy-MM-dd");
                                    mqttItems["Time"] = dt.ToString("HH:mm:ss"); ;
                                    mqttItems["Wind_Speed"] = opcdaItems[i]["WindSpeed"];
                                    mqttItems["Wind_Direction"] = opcdaItems[i]["WindDirection"];
                                    mqttItems["Module_Temperature_1"] = opcdaItems[i]["PVTemp1"];
                                    mqttItems["Module_Temperature_2"] = opcdaItems[i]["PVTemp2"];
                                    mqttItems["Module_Temperature_3"] = "";
                                    mqttItems["Ambient_Temperature_1"] = opcdaItems[i]["AmbTemp"];
                                    mqttItems["Ambient_Temperature_2"] = "";
                                    mqttItems["Ambient_Temperature_3"] = "";
                                    mqttItems["Ambient_Humidity_1"] = opcdaItems[i]["AmbHumidity"];
                                    mqttItems["Ambient_Humidity_2"] = "";
                                    mqttItems["Ambient_Humidity_3"] = "";
                                    mqttItems["Total_Irradiance_1"] = opcdaItems[i]["Irriadiation1"];
                                    mqttItems["Total_Irradiance_2"] = opcdaItems[i]["Irriadiation2"];
                                    mqttItems["Total_Irradiance_3"] = "";
                                    mqttItems["Daily_Irradiance_1"] = "";
                                    mqttItems["Daily_Irradiance_2"] = "";
                                    mqttItems["Daily_Irradiance_3"] = "";
                                    mqttItems["Daily_Irradiance_energy_1"] = "";
                                    mqttItems["Daily_Irradiance_energy_2"] = "";
                                    break;
                                case 3: //PQM
                                    mqttItems["ID"] = i.ToString();
                                    mqttItems["Date"] = dt.ToString("yyyy-MM-dd");
                                    mqttItems["Time"] = dt.ToString("HH:mm:ss"); ;
                                    mqttItems["Voltage_L1"] = opcdaItems[i]["VoltageLn1"];
                                    mqttItems["Voltage_L2"] = opcdaItems[i]["VoltageLn2"];
                                    mqttItems["Voltage_L3"] = opcdaItems[i]["VoltageLn3"];
                                    mqttItems["Voltage_L12"] = "";
                                    mqttItems["Voltage_L23"] = "";
                                    mqttItems["Voltage_L31"] = "";
                                    mqttItems["Current_L1"] = opcdaItems[i]["Current1"];
                                    mqttItems["Current_L2"] = opcdaItems[i]["Current2"];
                                    mqttItems["Current_L3"] = opcdaItems[i]["Current3"];
                                    mqttItems["Current_Total"] = "";
                                    mqttItems["Power_Active_L1"] = "";
                                    mqttItems["Power_Active_L2"] = "";
                                    mqttItems["Power_Active_L3"] = "";
                                    mqttItems["Power_Active_Total"] = opcdaItems[i]["TotalActivePower"];
                                    mqttItems["Power_Apparent_L1"] = "";
                                    mqttItems["Power_Apparent_L2"] = "";
                                    mqttItems["Power_Apparent_L3"] = "";
                                    mqttItems["Power_Apparent_Total"] = opcdaItems[i]["TotalApparentPower"];
                                    mqttItems["Power_Reactive_L1"] = "";
                                    mqttItems["Power_Reactive_L2"] = "";
                                    mqttItems["Power_Reactive_L3"] = "";
                                    mqttItems["Power_Reactive_Total"] = opcdaItems[i]["TotalReactivePower"];
                                    mqttItems["CosPhi_L1"] = opcdaItems[i]["VoltagePh1"];
                                    mqttItems["CosPhi_L2"] = opcdaItems[i]["VoltagePh2"];
                                    mqttItems["CosPhi_L3"] = opcdaItems[i]["VoltagePh3"];
                                    mqttItems["Power_Factor"] = opcdaItems[i]["PowerFactor"];
                                    mqttItems["Frequency"] = opcdaItems[i]["Frequency"];
                                    mqttItems["Energy_Active_Total"] = opcdaItems[i]["Energy"];
                                    mqttItems["Energy_Active_Import_L1"] = "";
                                    mqttItems["Energy_Active_Import_L2"] = "";
                                    mqttItems["Energy_Active_Import_L3"] = "";
                                    mqttItems["Energy_Active_Import_Total"] = "";
                                    mqttItems["Energy_Active_Export_L1"] = "";
                                    mqttItems["Energy_Active_Export_L2"] = "";
                                    mqttItems["Energy_Active_Export_L3"] = "";
                                    mqttItems["Energy_Active_Export_Total"] = "";
                                    mqttItems["Energy_Apparent_L1"] = "";
                                    mqttItems["Energy_Apparent_L2"] = "";
                                    mqttItems["Energy_Apparent_L3"] = "";
                                    mqttItems["Energy_Apparent_Total"] = "";
                                    mqttItems["Energy_Reactive_L1"] = "";
                                    mqttItems["Energy_Reactive_L2"] = "";
                                    mqttItems["Energy_Reactive_L3"] = "";
                                    mqttItems["Energy_Reactive_Total"] = opcdaItems[i]["EnergyReactive"];
                                    break;
                                case 4: //MAIN
                                    mqttItems["ID"] = i.ToString();
                                    mqttItems["Date"] = dt.ToString("yyyy-MM-dd");
                                    mqttItems["Time"] = dt.ToString("HH:mm:ss"); ;
                                    mqttItems["SWG_Close"] = opcdaItems[i]["Close"];
                                    mqttItems["SWG_Open"] = opcdaItems[i]["Open"];
                                    mqttItems["SWG_Earth_Switch_Close"] = opcdaItems[i]["EarthSwitchClose"];
                                    mqttItems["SWG_Close_Switch_Open"] = opcdaItems[i]["EarthSwitchOpen"];
                                    mqttItems["SWG_local"] = opcdaItems[i]["Local"];
                                    mqttItems["SWG_Remote"] = opcdaItems[i]["Remote"];
                                    mqttItems["SWG_CB_Ready"] = opcdaItems[i]["CBReady"];
                                    mqttItems["SWG_CB_Service"] = opcdaItems[i]["CBService"];
                                    mqttItems["MAI_CB_Test"] = opcdaItems[i]["CBTest"];
                                    mqttItems["Fire_Alarm"] = opcdaItems[i]["FireAlarm"];
                                    mqttItems["Batt._Loss_AC_Input"] = opcdaItems[i]["BattLossACInput"];
                                    mqttItems["Batt._Charge_Fail"] = opcdaItems[i]["BattChargeFail"];
                                    mqttItems["Batt._Low_DC"] = opcdaItems[i]["BattLowDC"];
                                    mqttItems["Batt._High_DC"] = opcdaItems[i]["BattHighDC"];
                                    mqttItems["Commu._Status"] = "";
                                    break;
                                case 5:  //SUB
                                    mqttItems["ID"] = i.ToString();
                                    mqttItems["Date"] = dt.ToString("yyyy-MM-dd");
                                    mqttItems["Time"] = dt.ToString("HH:mm:ss");
                                    switch (i)
                                    {
                                        case 156: //RMUIn
                                            mqttItems["RMU-IN_DC_Switch_Close"] = opcdaItems[i]["DCSwitchClose"];
                                            mqttItems["RMU-IN_DC_Switch_Open"] = opcdaItems[i]["DCSwitchOpen"];
                                            mqttItems["RMU-IN_Earth_Close/Open"] = opcdaItems[i]["EarthCloseOpen"];
                                            mqttItems["RMU-IN_Remote/Local"] = opcdaItems[i]["RemoteLocal"];
                                            mqttItems["RMU-IN_Pressure_Low_Normal"] = opcdaItems[i]["PressureLowNormal"];

                                            mqttItems["RMU-OUT1_CB_Switch_Close"] = "";
                                            mqttItems["RMU-OUT1_CB_Switch_Open"] = "";
                                            mqttItems["RMU-OUT1_Earth_Close/Open"] = "";
                                            mqttItems["RMU-OUT1_CB_Remote/Local"] = "";
                                            mqttItems["RMU-OUT1_CB_Pressure_Low_Normal"] = "";
                                            mqttItems["RMU-OUT1_DC_Switch_Close"] = "";
                                            mqttItems["RMU-OUT1_DC_Switch_Open"] = "";
                                            mqttItems["RMU-OUT1_Spring_Chage_Ready"] = "";

                                            mqttItems["GasAlarm"] = "";
                                            mqttItems["PressureAlarm"] = "";
                                            mqttItems["Temp1Alarm"] = "";
                                            mqttItems["Temp2Alarm"] = "";
                                            mqttItems["ACBClose"] = "";
                                            mqttItems["ACBOpen"] = "";
                                            mqttItems["ACBTrip"] = "";
                                            break;
                                        case 157://RMUOut1
                                        case 158://RMUOut2 
                                            mqttItems["RMU-IN_DC_Switch_Close"] = "";
                                            mqttItems["RMU-IN_DC_Switch_Open"] = "";
                                            mqttItems["RMU-IN_Earth_Close/Open"] = "";
                                            mqttItems["RMU-IN_Remote/Local"] = "";
                                            mqttItems["RMU-IN_Pressure_Low_Normal"] = "";

                                            mqttItems["RMU-OUT1_CB_Switch_Close"] = opcdaItems[i]["CBSwitchClose"];
                                            mqttItems["RMU-OUT1_CB_Switch_Open"] = opcdaItems[i]["CBSwithOpen"];
                                            mqttItems["RMU-OUT1_Earth_Close/Open"] = opcdaItems[i]["EarthCloseOpen"];
                                            mqttItems["RMU-OUT1_CB_Remote/Local"] = opcdaItems[i]["RemoteLocal"];
                                            mqttItems["RMU-OUT1_CB_Pressure_Low_Normal"] = opcdaItems[i]["PressureLowNormal"];
                                            mqttItems["RMU-OUT1_DC_Switch_Close"] = opcdaItems[i]["DCSwitchClose"];
                                            mqttItems["RMU-OUT1_DC_Switch_Open"] = opcdaItems[i]["DCSwitchOpen"];
                                            mqttItems["RMU-OUT1_Spring_Chage_Ready"] = opcdaItems[i]["SpringChargeReady"];

                                            mqttItems["GasAlarm"] = "";
                                            mqttItems["PressureAlarm"] = "";
                                            mqttItems["Temp1Alarm"] = "";
                                            mqttItems["Temp2Alarm"] = "";
                                            mqttItems["ACBClose"] = "";
                                            mqttItems["ACBOpen"] = "";
                                            mqttItems["ACBTrip"] = "";
                                            break;
                                        case 159://TR1
                                        case 160://TR2
                                            mqttItems["RMU-IN_DC_Switch_Close"] = "";
                                            mqttItems["RMU-IN_DC_Switch_Open"] = "";
                                            mqttItems["RMU-IN_Earth_Close/Open"] = "";
                                            mqttItems["RMU-IN_Remote/Local"] = "";
                                            mqttItems["RMU-IN_Pressure_Low_Normal"] = "";

                                            mqttItems["RMU-OUT1_CB_Switch_Close"] = "";
                                            mqttItems["RMU-OUT1_CB_Switch_Open"] = "";
                                            mqttItems["RMU-OUT1_Earth_Close/Open"] = "";
                                            mqttItems["RMU-OUT1_CB_Remote/Local"] = "";
                                            mqttItems["RMU-OUT1_CB_Pressure_Low_Normal"] = "";
                                            mqttItems["RMU-OUT1_DC_Switch_Close"] = "";
                                            mqttItems["RMU-OUT1_DC_Switch_Open"] = "";
                                            mqttItems["RMU-OUT1_Spring_Chage_Ready"] = "";

                                            mqttItems["RMU-OUT1_CB_Switch_Close"] = "";
                                            mqttItems["RMU-OUT1_CB_Switch_Open"] = "";
                                            mqttItems["RMU-OUT1_Earth_Close/Open"] = "";
                                            mqttItems["RMU-OUT1_CB_Remote/Local"] = "";
                                            mqttItems["RMU-OUT1_CB_Pressure_Low_Normal"] = "";
                                            mqttItems["RMU-OUT1_DC_Switch_Close"] = "";
                                            mqttItems["RMU-OUT1_DC_Switch_Open"] = "";
                                            mqttItems["RMU-OUT1_Spring_Chage_Ready"] = "";

                                            mqttItems["GasAlarm"] = opcdaItems[i]["GasAlarm"];
                                            mqttItems["PressureAlarm"] = opcdaItems[i]["PressureAlarm"];
                                            mqttItems["Temp1Alarm"] = opcdaItems[i]["Temp1Alarm"];
                                            mqttItems["Temp2Alarm"] = opcdaItems[i]["Temp2Alarm"];
                                            mqttItems["ACBClose"] = opcdaItems[i]["ACBClose"];
                                            mqttItems["ACBOpen"] = opcdaItems[i]["ACBOpen"];
                                            mqttItems["ACBTrip"] = opcdaItems[i]["ACBTrip"];
                                            break;
                                    }
                                    mqttItems["Commu._Status"] = "";
                                    break;
                                case 6:  //WIND.PQM
                                    mqttItems["ID"] = i.ToString();
                                    mqttItems["Date"] = dt.ToString("yyyy-MM-dd");
                                    mqttItems["Time"] = dt.ToString("HH:mm:ss");
                                    mqttItems["Voltage_L2"] = "";
                                    mqttItems["Voltage_L3"] = "";
                                    mqttItems["Voltage_L12"] = "";
                                    mqttItems["Voltage_L23"] = opcdaItems[i]["PT2_16"];
                                    mqttItems["Voltage_L31"] = opcdaItems[i]["PT2_17"];
                                    mqttItems["Current_L1"] = opcdaItems[i]["PT2_18"];
                                    mqttItems["Current_L2"] = opcdaItems[i]["CT2_10"];
                                    mqttItems["Current_L3"] = opcdaItems[i]["CT2_11"];
                                    mqttItems["Current_Total"] = opcdaItems[i]["CT2_12"];
                                    mqttItems["Power_Active_L1"] = "";
                                    mqttItems["Power_Active_L2"] = "";
                                    mqttItems["Power_Active_L3"] = "";
                                    mqttItems["Power_Active_Total"] = "";
                                    mqttItems["Power_Apparent_L1"] = opcdaItems[i]["PQ2_20"];
                                    mqttItems["Power_Apparent_L2"] = "";
                                    mqttItems["Power_Apparent_L3"] = "";
                                    mqttItems["Power_Apparent_Total"] = "";
                                    mqttItems["Power_Reactive_L1"] = "";
                                    mqttItems["Power_Reactive_L2"] = "";
                                    mqttItems["Power_Reactive_L3"] = "";
                                    mqttItems["Power_Reactive_Total"] = "";
                                    mqttItems["CosPhi_L1"] = opcdaItems[i]["PQ2_21"];
                                    mqttItems["CosPhi_L2"] = "";
                                    mqttItems["CosPhi_L3"] = "";
                                    mqttItems["Power_Factor"] = "";
                                    mqttItems["Frequency"] = opcdaItems[i]["PQ2_23"];
                                    mqttItems["Energy_Active_Total"] = opcdaItems[i]["PQ2_24"];
                                    break;
                                case 7:
                                    mqttItems["ID"] = i.ToString();
                                    mqttItems["Date"] = dt.ToString("yyyy-MM-dd");
                                    mqttItems["Time"] = dt.ToString("HH:mm:ss");
                                    mqttItems["WIND_Prod"] = "";
                                    mqttItems["Total_Prod"] = "";
                                    mqttItems["Year_Prod"] = "";
                                    mqttItems["Month_Prod"] = "";
                                    mqttItems["Prev_Month_Prod"] = "";
                                    mqttItems["Daily_Prod"] = "";
                                    mqttItems["Hour_Prod"] = "";
                                    mqttItems["Active_Power"] = "";
                                    mqttItems["Reactive_Power"] = "";
                                    mqttItems["Stator_Active_Power"] = opcdaItems[i]["ME1_10"];
                                    mqttItems["Stator_Reactive_Power"] = opcdaItems[i]["PQ1_20"];
                                    mqttItems["Stator_Active_Power_Setpoint"] = "";
                                    mqttItems["Stator_Reactive_Power_Setpoint"] = opcdaItems[i]["PQ1_21"];
                                    mqttItems["Aux_Active_Power"] = "";
                                    mqttItems["Aux_Reactive_Power"] = "";
                                    mqttItems["Ref_Active_Power"] = "";
                                    mqttItems["Ref_Reactive_Power"] = "";
                                    mqttItems["Wind_Speed"] = "";
                                    mqttItems["Amb_Temp"] = "";
                                    mqttItems["Generator_Temp_1"] = "";
                                    mqttItems["Generator_Temp_2"] = "";
                                    mqttItems["Generator_Temp_3"] = "";
                                    mqttItems["Gearbox_Bearing_Temp"] = "";
                                    mqttItems["Gearbox_Oil_Temp"] = "";
                                    mqttItems["Generator_Speed"] = opcdaItems[i]["ST1_10"];
                                    mqttItems["Turbine_Status"] = "";
                                    mqttItems["Turbine_States"] = "";
                                    mqttItems["Alarm_Code_1"] = "";
                                    mqttItems["Alarm_Code_2"] = "";
                                    mqttItems["Alarm_Code_3"] = "";
                                    mqttItems["Device_Status"] = opcdaItems[i]["ST1_11"];
                                    break;
                                case 8:
                                    mqttItems["ID"] = i.ToString();
                                    mqttItems["Date"] = dt.ToString("yyyy-MM-dd");
                                    mqttItems["Time"] = dt.ToString("HH:mm:ss");
                                    mqttItems["Voltage_L1"] = "";
                                    mqttItems["Voltage_L2"] = "";
                                    mqttItems["Voltage_L3"] = "";
                                    mqttItems["Voltage_L12"] = opcdaItems[i]["PT2_16"];
                                    mqttItems["Voltage_L23"] = opcdaItems[i]["PT2_17"];
                                    mqttItems["Voltage_L31"] = opcdaItems[i]["PT2_18"];
                                    mqttItems["Current_L1"] = opcdaItems[i]["CT2_10"];
                                    mqttItems["Current_L2"] = opcdaItems[i]["CT2_11"];
                                    mqttItems["Current_L3"] = opcdaItems[i]["CT2_12"];
                                    mqttItems["Current_Total"] = "";
                                    mqttItems["Power_Active_L1"] = "";
                                    mqttItems["Power_Active_L2"] = "";
                                    mqttItems["Power_Active_L3"] = "";
                                    mqttItems["Power_Active_Total"] = opcdaItems[i]["PQ2_20"];
                                    mqttItems["Power_Apparent_L1"] = "";
                                    mqttItems["Power_Apparent_L2"] = "";
                                    mqttItems["Power_Apparent_L3"] = "";
                                    mqttItems["Power_Apparent_Total"] = "";
                                    mqttItems["Power_Reactive_L1"] = "";
                                    mqttItems["Power_Reactive_L2"] = "";
                                    mqttItems["Power_Reactive_L3"] = "";
                                    mqttItems["Power_Reactive_Total"] = opcdaItems[i]["PQ2_21"];
                                    mqttItems["CosPhi_L1"] = "";
                                    mqttItems["CosPhi_L2"] = "";
                                    mqttItems["CosPhi_L3"] = "";
                                    mqttItems["Power_Factor"] = opcdaItems[i]["PQ2_23"];
                                    mqttItems["Frequency"] = opcdaItems[i]["PQ2_24"];
                                    mqttItems["Energy_Active_Total"] = "";
                                    break;
                            }
                            jsonPayload = JsonConvert.SerializeObject(mqttItems);
                            var message = new MqttApplicationMessageBuilder()
                                .WithTopic(topic[i])
                                .WithPayload(jsonPayload)
                                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce)
                                .WithRetainFlag(false)
                                .Build();
                            await mqttClient.PublishAsync(message);
                            //Console.WriteLine("MQTT {0} published json: {1}", topic[i], jsonPayload);
                            Console.WriteLine("MQTT {0} published DT: {1}", topic[i], dt.ToString("yyyy-MM-dd HH:mm:ss"));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred while publishing MQTT: {0}", ex.Message);
            }
        }
        static void PrintErrors(OpcDaItemResult[] results, string gname)
        {
            foreach (var result in results)
            {
                if (result.Error.Failed)
                    Console.WriteLine("Error adding item '{0}': {1}", gname, result.Error);
            }
        }
        static string GetTextAfterLastDot(string input) //TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub2.SINV34.VoltageA => VoltageA
        {
            int dotIndex = input.LastIndexOf('.');
            return dotIndex != -1 ? input.Substring(dotIndex + 1) : input;
        }
        static string GetTextAfterLastUnderscore(string input) //TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET9_10 => 10
        {
            int underscoreIndex = input.LastIndexOf('_');
            return underscoreIndex != -1 ? input.Substring(underscoreIndex + 1) : input;
        }
        static string GetTextAfterSecondUnderscore(string input) //TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET9_10 => MET9_10
        {
            int secondFromLastUnderscoreIndex = input.LastIndexOf('_', input.LastIndexOf('_') - 1);
            return secondFromLastUnderscoreIndex != -1 ?
                   input.Substring(0, secondFromLastUnderscoreIndex) + "_" + input.Substring(secondFromLastUnderscoreIndex + 1) :
                   input;
        }
        static string GetTextAfterSecondDot(string input) ////TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub2.SINV34.VoltageA => SINV34.VoltageA
        {
            int firstDotIndex = input.IndexOf('.');
            if (firstDotIndex != -1)
            {
                int secondDotIndex = input.IndexOf('.', firstDotIndex + 1);
                if (secondDotIndex != -1)
                {
                    return input.Substring(secondDotIndex + 1);
                }
            }
            return input;
        }
    }
}
