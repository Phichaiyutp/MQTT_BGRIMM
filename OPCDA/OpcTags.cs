//OpcTags.cs
using System.Collections.Generic;
using TitaniumAS.Opc.Client.Da;

namespace OPCDA
{
    internal class OpcTags
    {
        public static List<OpcDaItemDefinition> ItemId_WT()
        {
            var itemList = new List<OpcDaItemDefinition>();
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.WT.Irradiation1", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.WT.Irradiation2", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.WT.PVTemp1", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.WT.PVTemp2", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.WT.AmbTemp", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.WT.AmbHumidity", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.WT.WindSpeed", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.WT.WindDirection", IsActive = true });
            return itemList;
        }
        public static List<OpcDaItemDefinition> ItemId_PQM()
        {
            var itemList = new List<OpcDaItemDefinition>();
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.Frequency", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.VoltageLn0", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.VoltageLn1", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.VoltageLn2", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.VoltageLn3", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.VoltagePh0", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.VoltagePh1", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.VoltagePh2", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.VoltagePh3", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.VoltageNtoG", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.Current1", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.Current2", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.Current3", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.Energy", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.EnergyReactive", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.TotalActivePower", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.TotalReactivePower", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.TotalApparentPower", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.PQM.PowerFactor", IsActive = true });
            return itemList;
        }
        public static List<OpcDaItemDefinition> ItemId_MAIN()
        {
            var itemList = new List<OpcDaItemDefinition>();
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.SWG.Close", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.SWG.Open", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.SWG.EarthSwitchClose", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.SWG.EarthSwitchOpen", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.SWG.Local", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.SWG.Remote", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.SWG.CBReady", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.SWG.CBService", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.SWG.CBTest", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.FireAlarm", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.BattLossACInput", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.BattChargeFail", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.BattLowDC", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.BattHighDC", IsActive = true });
            return itemList;
        }
        public static List<OpcDaItemDefinition> ItemId_SUB()
        {
            var itemList = new List<OpcDaItemDefinition>();
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUIn.DCSwitchClose", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUIn.DCSwitchOpen", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUIn.EarthCloseOpen", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUIn.RemoteLocal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUIn.PressureLowNormal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut1.CBSwitchClose", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut1.CBSwithOpen", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut1.EarthCloseOpen", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut1.RemoteLocal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut1.PressureLowNormal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut1.DCSwitchClose", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut1.DCSwitchOpen", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut1.SpringChargeReady", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut2.CBSwitchClose", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut2.CBSwithOpen", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut2.EarthCloseOpen", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut2.RemoteLocal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut2.PressureLowNormal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut2.DCSwitchClose", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut2.DCSwitchOpen", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.CtrlRoom.RMUOut2.SpringChargeReady", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.TR1.GasAlarm", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.TR1.PressureAlarm", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.TR1.Temp1Alarm", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.TR1.Temp2Alarm", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.TR1.ACBClose", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.TR1.ACBOpen", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.TR1.ACBTrip", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub2.TR2.GasAlarm", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub2.TR2.PressureAlarm", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub2.TR2.Temp1Alarm", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub2.TR2.Temp2Alarm", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub2.TR2.ACBClose", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub2.TR2.ACBOpen", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub2.TR2.ACBTrip", IsActive = true });
            return itemList;
        }

        public static List<OpcDaItemDefinition> ItemId_SINV()
        {
            var itemList = new List<OpcDaItemDefinition>();
            for (int i = 1; i <= 38; i++)
            {
                string Sub1Prefix = $"TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub1.SINV{i:00}";
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.NominalActivePower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.NominalRactivePower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.DailyPowerYield", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.MonthlyPowerYield", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.TotalPowerYield", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.TotalActivePower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.TotalReactivePower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.TotalAppearentPower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.PowerFactor", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.GridFrequency", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.VoltageA", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.VoltageB", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.VoltageC", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.CurrentA", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.CurrentB", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.CurrentC", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.TotalDCPower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.DCVoltage", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.DCCurrent", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent2", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent3", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent4", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent5", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent6", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent7", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent8", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent9", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent11", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent12", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent13", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.StringCurrent14", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.DailyRunningTime", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.TotalRunningTime", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.BusVoltage", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub1Prefix}.Internal_temperature", IsActive = true });
                string Sub2Prefix = $"TH00P01PVMPIS01\\S-TH-BBO-Modbus.Sub2.SINV{i:00}";
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.NominalActivePower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.NominalRactivePower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.DailyPowerYield", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.MonthlyPowerYield", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.TotalPowerYield", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.TotalActivePower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.TotalReactivePower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.TotalAppearentPower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.PowerFactor", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.GridFrequency", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.VoltageA", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.VoltageB", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.VoltageC", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.CurrentA", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.CurrentB", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.CurrentC", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.TotalDCPower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.DCVoltage", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.DCCurrent", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent2", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent3", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent4", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent5", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent6", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent7", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent8", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent9", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent11", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent12", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent13", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.StringCurrent14", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.DailyRunningTime", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.TotalRunningTime", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.BusVoltage", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{Sub2Prefix}.Internal_temperature", IsActive = true });
            }
            return itemList;
        }
        public static List<OpcDaItemDefinition> ItemId_AC()
        {
            var itemList = new List<OpcDaItemDefinition>();
            for (int i = 1; i <= 2; i++)
            {
                string ACPrefix = $"TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.AC{i:00}";
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_AliveCounter", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_BusbarVoltage", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Connectivity", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_DeltaPwrResAvailPwr", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Frecuencia", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_FrecuenciaParqueAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_FrecuenciaParqueDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_FrecuenciaParqueMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_FrecuenciaParqueMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_NumWTGFalloCom", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_NumWTGLim", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_P_Total_Parque", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_P_Total_ParqueAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_P_Total_ParqueAv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_P_Total_ParqueDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_P_Total_ParqueDv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_P_Total_ParqueMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_P_Total_ParqueMn15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_P_Total_ParqueMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_P_Total_ParqueMx15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_P_Total_ParqueWF", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Pnominal", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_PorcRegulEnviada", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_PotenciaParada", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Pproducible", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Q_Total_Parque", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Q_Total_ParqueAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Q_Total_ParqueAv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Q_Total_ParqueDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Q_Total_ParqueDv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Q_Total_ParqueMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Q_Total_ParqueMn15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Q_Total_ParqueMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_Q_Total_ParqueMx15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_RegulPorcSolarInst", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_TensionEnBarrasSTA10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_TensionEnBarrasSTA15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_TensionEnBarrasSTD10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_TensionEnBarrasSTD15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_TensionEnBarrasSTN10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_TensionEnBarrasSTN15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_TensionEnBarrasSTX10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_TensionEnBarrasSTX15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_WFLimited", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_WFLimitedWng", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{ACPrefix}_WindSpeed", IsActive = true });
                string MDPrefix = $"TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.MD{i:00}";
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ActivePower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ActivePowerAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ActivePowerR", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ActivePowerS", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ActivePowerT", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ApparentPower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ApparentPowerR", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ApparentPowerS", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ApparentPowerT", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_CosPhi", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_CosPhiAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_CosPhiR", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_CosPhiS", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_CosPhiT", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_Current", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_CurrentR", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_CurrentS", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_CurrentT", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_Frequency", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_FrequencyAv", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_FrequencyAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ReactivePower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ReactivePowerAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ReactivePowerR", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ReactivePowerS", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_ReactivePowerT", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_Voltage", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_VoltageAd", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_VoltageAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_VoltageR", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_VoltageRS", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_VoltageS", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_VoltageST", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_VoltageT", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{MDPrefix}_VoltageTR", IsActive = true });
                string REPrefix = $"TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.RE{i:00}";
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_BusbarVoltage", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_ContadorVida", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_NumWTGFalloCom", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_P_Total_Parque", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_P_Total_ParqueAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_P_Total_ParqueAv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_P_Total_ParqueDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_P_Total_ParqueDv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_P_Total_ParqueMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_P_Total_ParqueMn15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_P_Total_ParqueMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_P_Total_ParqueMx15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_Q_Total_Parque", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_Q_Total_ParqueAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_Q_Total_ParqueAv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_Q_Total_ParqueDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_Q_Total_ParqueDv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_Q_Total_ParqueMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_Q_Total_ParqueMn15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_Q_Total_ParqueMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_Q_Total_ParqueMx15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_ReactKVArSPSentWTG", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_TensionEnBarrasSTA10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_TensionEnBarrasSTA15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_TensionEnBarrasSTD10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_TensionEnBarrasSTD15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_TensionEnBarrasSTN10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_TensionEnBarrasSTN15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_TensionEnBarrasSTX10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_TensionEnBarrasSTX15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_VQStaticSPApplied", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_WFCosPhi", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_WFCosPhiAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_WFCosPhiDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_WFCosPhiMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{REPrefix}_WFCosPhiMx10", IsActive = true });
            }


            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.AmbientTemp", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.DisponMensual", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.DisponMesAnterior", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.DisponTotal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.Eficiencia", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.EnergiaAnual", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.EnergiaMensual", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.EnergiaTotal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.FactorCapacidad", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.Incidencias", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.KeepAlive", IsActive = true });

            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.Num_Active_Clients", IsActive = true });

            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_AlarmCounter", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_AmbientTemp", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_AmbientTempMax", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_AmbientTempMin", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_DisponMensual", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_DisponMesAnterior", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_DisponTotal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_Eficiencia", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_EnergiaAnual", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_EnergiaMensual", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_FactorCapacidad", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_Incidencias", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_NumWTGCommFail", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_NumWTGLimDegraded", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_NumWTGOnline", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_NumWTGPause", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_NumWTGRunReady", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_NumWTGStopEmerg", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_PotenciaGeneral", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_PotenciaGeneralAv10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_PotenciaGeneralDv10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_PotenciaGeneralMn10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_PotenciaGeneralMx10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_PotenciaGeneralTot", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_ProduccionTotal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_WarningCounter", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_WindDirection", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_WindDirectionMax", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_WindDirectionMin", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_WindSpeed", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_WindSpeedMax", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR_WindSpeedMin", IsActive = true });

            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_AlarmCounter", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_AmbientTemp", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_AmbientTempMax", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_AmbientTempMin", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_DisponMensual", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_DisponMesAnterior", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_DisponTotal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_Eficiencia", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_EnergiaAnual", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_EnergiaMensual", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_FactorCapacidad", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_Incidencias", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_NumWTGCommFail", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_NumWTGLimDegraded", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_NumWTGOnline", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_NumWTGPause", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_NumWTGRunReady", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_NumWTGStopEmerg", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_PotenciaGeneral", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_PotenciaGeneralAv10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_PotenciaGeneralDv10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_PotenciaGeneralMn10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_PotenciaGeneralMx10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_PotenciaGeneralTot", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_ProduccionTotal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_VelVientoMn", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_VelVientoMx", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_WarningCounter", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_WindDirection", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_WindDirectionMax", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_WindDirectionMin", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PR01_WindSpeed", IsActive = true });

            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PotenciaGeneral", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PotenciaGeneralAv10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PotenciaGeneralDv10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PotenciaGeneralMn10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PotenciaGeneralMx10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PotenciaGeneralTot", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PotenciaMedia10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.PotenciaProducible10", IsActive = true });



            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_AlarmCounter", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_AmbientTemp", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_AmbientTempMax", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_AmbientTempMin", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_DisponMensual", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_DisponMesAnterior", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_DisponTotal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_Eficiencia", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_EnergiaAnual", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_EnergiaMensual", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_EnergiaTotal", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_FactorCapacidad", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_Incidencias", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_NumWTGCommFail", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_NumWTGLimDegraded", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_NumWTGOnline", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_NumWTGPause", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_NumWTGRunReady", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_NumWTGStopEmerg", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_PotenciaGeneral", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_PotenciaGeneralAv10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_PotenciaGeneralDv10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_PotenciaGeneralMn10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_PotenciaGeneralMx10", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_PotenciaGeneralTot", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_VelVientoMn", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_VelVientoMx", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_WarningCounter", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_WindDirection", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_WindDirectionMax", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_WindDirectionMin", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.SB01_WindSpeed", IsActive = true });

            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.System_UpTime", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.Tiempo", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.UPS_EstadoCom", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.UPS_TiempoBaterias", IsActive = true });
            return itemList;
        }
        public static List<OpcDaItemDefinition> ItemId_WTG()
        {
            //wind turbine gamesa (wtg)
            var itemList = new List<OpcDaItemDefinition>();
            for (int i = 1; i <= 4; i++)
            {
                string WTGPrefix = $"TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.WTG{i}";
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_Direccion", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_DireccionAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_DireccionAv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_DireccionDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_DireccionDv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_DireccionMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_DireccionMn15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_DireccionMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_DireccionMx15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_ErrorSensorHielo", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_PausaMesActualHielo", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_PausaMesAntHielo", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_PausaTotalHielo", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_SensorHieloHabilita", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_SensorHieloPresente", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_Temp", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_TempAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_TempAv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_TempDeteccionHielo", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_TempDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_TempDv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_TempMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_TempMn15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_TempMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_TempMx15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VelViento", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VelVientoAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VelVientoAv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VelVientoDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VelVientoDv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VelVientoMedia", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VelVientoMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VelVientoMn15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VelVientoMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VelVientoMx15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VelVientoSinFiltrar", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VientoDerecho", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VientoFrontal", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VientoIzquierdo", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Amb_VientoTrasero", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Aux_Estado", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Aux_VerPLC", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_CambioParmsCod1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_ContEnAcuExtraPower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Conv_Alim400VNoPresente", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Conv_AltaFrecRed", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Conv_BajaTensRed", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Conv_DataTrackerGrabado", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Conv_HuecoTension", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Conv_PotReactivaGDF", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_DeltaControlSP", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_DeltaControlStatus", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_DensidadAireEst", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Disp_Rec_Amb", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Disp_Rec_Mes", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Disp_Rec_MesAmb", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Disp_Rec_MesAnterior", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Disp_Rec_MesAnteriorAmb", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Disp_Rec_MesAnteriorRed", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Disp_Rec_MesAnteriorTurbina", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Disp_Rec_MesRed", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Disp_Rec_MesTurbina", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Disp_Rec_Total", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Disp_Rec_TotalRed", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Disp_Rec_TotalTurbina", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_EnConsDiaActual", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_EnConsDiaAnterior", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_EnConsMesActual", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_EnConsMesAnterior", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_EnConsTotal", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_ErrorComunicaciones", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Estado", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_EstadoMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_EstadoMn15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Freno_Aplicado", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Freno_FalloTermostato", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gem_GUYSPLCWord", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_AnillosTemp", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_AnillosTempAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_AnillosTempDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_AnillosTempMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_AnillosTempMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CiclosLubRodAnno", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CojLATemp", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CojLATempAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CojLATempDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CojLATempMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CojLATempMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CojLOATemp", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CojLOATempAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CojLOATempDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CojLOATempMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CojLOATempMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_ConsignaCosPhi", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_ConsignaCosPhiAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_ConsignaCosPhiDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_ConsignaCosPhiMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_ConsignaCosPhiMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_ConsignaPEst", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_ConsignaQEst", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CorrEstatorP1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_CorrInvRedP1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_EstBombaLubricPalas", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_EstBombaLubricRod", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_LimPotActEstatorDisp", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_LimTermQEstator", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PctGrasaTanqLubRod", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotActivaAux", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotEstator", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotEstatorAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotEstatorDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotEstatorMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotEstatorMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotReactiva", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotReactivaAux", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotReactivaAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotReactivaAv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotReactivaDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotReactivaDv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotReactivaEstator", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotReactivaMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotReactivaMn15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotReactivaMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotReactivaMx15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_Potencia", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotenciaAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotenciaAv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotenciaDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotenciaDv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotenciaMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotenciaMn15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotenciaMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_PotenciaMx15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_RefPotActiva", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_RefPotReactiva", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase1Av10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase1Dv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase1Mn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase1Mx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase2", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase2Av10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase2Dv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase2Mn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase2Mx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase3", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase3Av10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase3Dv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase3Mn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TempDevFase3Mx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_Tipo", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TopVel", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TopVelAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TopVelDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TopVelMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_TopVelMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gen_VelDispOGS", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Generales_HorasServicio", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_Autoyaw90ON", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_BombaRegOn", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_CiclosLubPinsAnno", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_CiclosLubPlacasAnno", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_DeshabAutoYaw", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_EstBombaLubricPins", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_ManualCCW", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_ManualCW", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_MotorCCWOn", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_MotorCWOn", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_NingunSensorYaw", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_PctGrasaTanqLubPins", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_PctGrasaTanqLubPlaca", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_Posicion", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_PosicionAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_PosicionDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_PosicionMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_PosicionMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_PresFrenoYaw", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_SensorYawCCW", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_SensorYawCCWyNOStop", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_SensorYawCCWyStop", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_SensorYawCW", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_SensorYawCWyNOStop", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_SensorYawCWyStop", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_SensorYawStop", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Giro_TipoFrenoYaw", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gon_Temp", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_AlturaTorre", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_BrakeChopperOn", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_CambParametros", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_BusTension", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_DatosSMP", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_GrdSFVel", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_RefPotReactInvRed", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_TempAmbConv", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_TempAmbKM52E", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_TempAmbMagEst", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_TempAreaBChopRes", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_TempAreaCompSkiips", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_TempAreaFG005", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_TempArmDist", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_TempInducRed", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_TempR1InvRed", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_TempR2InvRed", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_TempR3InvRed", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Ctr_TempTrafoAux", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_ControlReactiva", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_CrowAcOn", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_DeratingTempAmbON", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_LimCosphi", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_LimPotFrecPrimariaON", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_LimPotenciaActiva", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_MejoraCornerON", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_P0QControl", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_P0QControlRequest", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_PLimitadaPorSCADA", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_PotLimActFrec", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_PotNom2Decimales", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_QCapacitivaConvLim", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_QCapacitivaGenLim", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_QInducitivaGenLim", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_QInductivaConvLim", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_RegulacionPorCPC", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_SafeModeON", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_SatQTensionAlta", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_SatQTensionBaja", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_SatQcapacitiva", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_SatQinductiva", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Pot_WTGListoParaCtrlV", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_Red_CaidaTensRed", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_TipoTorre", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_VersProgRFC", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_VersionProgramaPant", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_VersionRFC1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_VersionRFC2", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Gro_VersionRFC3", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Hidr_FalloNivel", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Hidr_FiltroHidr", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Hidr_Presion", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Hidr_Temp", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Hidr_TensionSalValvula", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_HorasTurbProdAct", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_IPSActivo", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_LPTAActLimPotAct", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_LPTCActLimPotAct", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Local", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_ModoBalizaCmdFdbk", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_Calefaccion", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_CiclosLubRodEjeAnno", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_EntradaFiltroSup", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_EstBombaLubricRodEje", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_FlujoParticulas", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_Numparticulas10m", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_PctGrasaTanqLubRodE", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_Temp", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_TempAceite", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_TempAceiteAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_TempAceiteDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_TempAceiteMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_TempAceiteMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_TempAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_TempDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_TempMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_TempMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_TempRodFront", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_TempRodTras", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Mul_TipoRefr", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_NivelRuido", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_NivelRuidoBajoViento", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_NivelRuidoFComm", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_ParamHabCtrlExtBalizaObe", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_ParametroMaxPotencia", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_PowerLimited", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_AnnoActual", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_DiaActual", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_EnergiaProducible", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_EnergiaProducida1h", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_HoraActual", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_MesActual", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_MesAnterior", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_PotenciaProducible", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_PotenciaProducible10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_PotenciaProducible1h", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_QCapProducible", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_QIndProducible", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Prod_Total", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_ProfinetComSt", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_RMSAvgInvRotF1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_RMSAvgInvRotF2", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_RMSAvgInvRotF3", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_AltaCorrNeutrTrafoAux", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_CmdSTOPConv", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_CorrLinea", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_FalloSistRefrPanCtrl", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_FalloSistRefrPanPot", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_Frec", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_FrecPLC", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_PotActivaInv", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_PotReactivaInv", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_Potencia", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_Tension", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_TensionAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_TensionAv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_TensionDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_TensionDv15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_TensionMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_TensionMn15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_TensionMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_TensionMx15", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Red_TensionRMSF1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Refr_TempRefrig", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Reg_AeroInclExclAct", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Reg_AeroInclExclReact", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_RegPPcntSetPoint", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_RegQSetPoint", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_RegulStatus", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_ReqDisRegCircElectrov", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_AnguloPitch", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_AnguloPitchAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_AnguloPitchDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_AnguloPitchMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_AnguloPitchMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_AnguloPitchPala1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_AnguloPitchPala2", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_AnguloPitchPala3", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_CiclosLubRodPala1Anno", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_CiclosLubRodPala2Anno", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_CiclosLubRodPala3Anno", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_ConPitch", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_ConVelRotor", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_Diametro", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_EmergenciaPitch", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_EstBombaLubricRdPala", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_PctGrasaTanLubRdPala", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_TipoCilindroPitch", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_Vel", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_VelAv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_VelDv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_VelMn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Rotor_VelMx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SP_StrategyStatus", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SP_StrategyStatus2", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SP_StrategyStatus3", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SP_StrategyStatus4", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SP_StrategyStatus5", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_BoltsMonSys", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_CBET", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_Corner", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_CorrOffPitchInj", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_CorrOffYawInj", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_Derating", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_DetOffPitchCol", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_DetOffYaw", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_EnergyThrust", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_ExtVRange", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_ExtraPower", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_ExtraPower3", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_FlexRatingLic1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_FlexRatingLic2", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_FlexRatingLic3", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_FlexRatingLic4", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_GPA99", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_LimitTempMS", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_LubRodGen", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_LubrDriveYaw", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_LubrRodPala", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_LubrRodPral", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_LubrRodYaw", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_MejoraOrient", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_MejoraQP0", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_MejoraQPdist0", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_MulMetalScan", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_OWI", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_PitchConst", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_QProdExtVRange", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_SafeMode", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_SensorSonico", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SPPF_TestValvProp", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_SatBitsStatus", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_StateRequested", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Switchgear_Modelo", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_TempIndInvB1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_TempIndInvB2", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_TempInvRotBAF1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_TempInvRotBAF2", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_TempInvRotBAF3", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_TempInvRotBBF1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_TempInvRotBBF2", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_TempInvRotBBF3", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_TipoAceite", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_Emergencia", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_FalloAlimen", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_HuecoTension", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_MaquinaEnEmergencia", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_MaquinaEnPausa", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_MaquinaEnStop", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_Marcha", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_PLCTensionOK", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_Parada", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_Pausa", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_SolicitudEmergTlm", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_SolicitudMarcha", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_SolicitudPausaTlm", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_SolicitudReposTlm", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_SolicitudReset", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_SolicitudRunTlm", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_TempAmbSonico", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_TipoAnemo", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_TipoKT", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_TipoNGR", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Top_TipoVeleta", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_EstContactorCelda", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev1", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev1Av10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev1Dv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev1Mn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev1Mx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev2", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev2Av10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev2Dv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev2Mn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev2Mx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev3", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev3Av10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev3Dv10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev3Mn10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Trf_TempDev3Mx10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_Anno", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_ControlON", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_Dia", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_FrecNatTorre", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_Hora", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_IntensEstimada10M", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_Mes", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_Minuto", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_NoOk", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_PotenciaNominal", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_SCADAMaster", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_Segundo", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_TipoMultiplicadora", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_TipoPala", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_TipoTrafo", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_VelViento10M", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_Turb_VersionProducto", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_UY_Habilitado", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_UY_Seleccion", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_VR_AlarmaSaturacionQCMax", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_VR_AlarmaSaturacionQIMax", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_VR_AlarmaSaturacionTemp", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_VR_AlarmaSaturacionVAlta", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_VR_AlarmaSaturacionVBaja", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_VR_EstadoHabilitacion", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_WTGStatusBits", IsActive = true });
            }

            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.WindDirection", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.WindSpeed", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.WindSpeedMax", IsActive = true });
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-Gamesa.D1.WindSpeedMin", IsActive = true });
            return itemList;
        }
        public static List<OpcDaItemDefinition> ItemId_W_PQM()
        {
            var itemList = new List<OpcDaItemDefinition>();
            for (int i = 1; i <= 2; i++)
            {
                string PQMPrefix = $"TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_PQM{i}";  //BWF J03-BT1/2 OUT PQM1/2
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_CT1_10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_CT1_11", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_CT1_12", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_EN1_27", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_EN1_28", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_EN2_27", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_EN2_28", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_PQ1_20", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_PQ1_21", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_PQ1_23", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_PT1_16", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_PT1_17", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_PT1_18", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{PQMPrefix}_PT1_24", IsActive = true });
            }
            return itemList;
        }
        public static List<OpcDaItemDefinition> ItemId_W_PM()
        {
            var itemList = new List<OpcDaItemDefinition>();
            for (int i = 1; i <= 2; i++)
            {
                string OUTPrefix = $"TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_OUT{i}"; //BWF J03-BT1/2 OUT DPM
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{OUTPrefix}_CT2_10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{OUTPrefix}_CT2_11", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{OUTPrefix}_CT2_12", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{OUTPrefix}_PQ2_20", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{OUTPrefix}_PQ2_21", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{OUTPrefix}_PQ2_23", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{OUTPrefix}_PQ2_24", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{OUTPrefix}_PT2_16", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{OUTPrefix}_PT2_17", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{OUTPrefix}_PT2_18", IsActive = true });

                string RCSPrefix = $"TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_RCS{i}"; //BWF BT1/2-FRTU
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{RCSPrefix}_CT1_10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{RCSPrefix}_CT1_11", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{RCSPrefix}_CT1_12", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{RCSPrefix}_PQ1_20", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{RCSPrefix}_PQ1_21", IsActive = true }); //not frq
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{RCSPrefix}_PQ1_23", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{RCSPrefix}_PT1_16", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{RCSPrefix}_PT1_17", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{RCSPrefix}_PT1_18", IsActive = true });

                string CAPPrefix = $"TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_CAP{i}"; //BWF J04-BT1/2 SVG DPM
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{CAPPrefix}_CT1_10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{CAPPrefix}_CT1_11", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{CAPPrefix}_CT1_12", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{CAPPrefix}_PQ1_20", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{CAPPrefix}_PQ1_21", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{CAPPrefix}_PQ1_23", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{CAPPrefix}_PQ1_24", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{CAPPrefix}_PT1_16", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{CAPPrefix}_PT1_17", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{CAPPrefix}_PT1_18", IsActive = true });
            }
            for (int i = 1; i <= 4; i++)
            {
                /*
                 * BWF J0x-BT1 SST DPM :  Diesel BT1
                 * BWF J0x-BT1 WTG1&2 DPM : Wind turbine BT1
                 * BWF J0x-BT2 SST DPM : Diesel BT2
                 * BWF J0x-BT1 WTG3&4 DPM : Wind turbine BT2
                 */
                string INCPrefix = $"TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_INC{i}";
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{INCPrefix}_CT1_10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{INCPrefix}_CT1_11", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{INCPrefix}_CT1_12", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{INCPrefix}_PQ1_20", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{INCPrefix}_PQ1_21", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{INCPrefix}_PQ1_23", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{INCPrefix}_PQ1_24", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{INCPrefix}_PT1_16", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{INCPrefix}_PT1_17", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{INCPrefix}_PT1_18", IsActive = true });
            }
            return itemList;
        }
        public static List<OpcDaItemDefinition> ItemId_W_WTG()
        {
            var itemList = new List<OpcDaItemDefinition>();
            for (int i = 1; i <= 4; i++)
            {
                string WTGPrefix = $"TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG{i:00}"; //BWF WTG-01/4
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_ME1_10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_PQ1_20", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_PQ1_21", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_ST1_10", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_ST1_11", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_WIND1_27", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGPrefix}_WIND1_28", IsActive = true });
            }

            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET1_10", IsActive = true }); //WTG_MET1
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET10_10", IsActive = true }); //WTG_MET10
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET11_10", IsActive = true }); //WTG_MET11
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET2_10", IsActive = true }); //WTG_MET2
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET3_10", IsActive = true }); //WTG_MET3
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET4_10", IsActive = true }); //WTG_MET4
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET5_10", IsActive = true }); //WTG_MET5
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET6_10", IsActive = true }); //WTG_MET6
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET7_10", IsActive = true }); //WTG_MET7
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET8_10", IsActive = true }); //WTG_MET8
            itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_MET9_10", IsActive = true }); //WTG_MET9

            for (int i = 1; i <= 2; i++)
            {
                string WTGTPrefix = $"TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_WTG_TOTAL{i}"; //BWF WTG TOTAL
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGTPrefix}_27", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGTPrefix}_28", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGTPrefix}_29", IsActive = true });
                itemList.Add(new OpcDaItemDefinition { ItemId = $"{WTGTPrefix}_30", IsActive = true });
            }
            return itemList;
        }
    }
}
/*
    itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_GEN1_ME1_28", IsActive = true }); //BWF BT1-GEN

    itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_SVG1_ST1_10", IsActive = true }); //BWF BT1-SVG
    itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_SVG1_ST1_11", IsActive = true }); //BWF BT1-SVG
    itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_SYS_N0003I_10", IsActive = true }); //NCC NET 3
    itemList.Add(new OpcDaItemDefinition { ItemId = "TH00P01PVMPIS01\\W-TH-BTW-MicroSCADA.D1.APL_1_P_SYS_N0004I_10", IsActive = true }); //NCC NET 4


 
    
    
}
}
}


*/