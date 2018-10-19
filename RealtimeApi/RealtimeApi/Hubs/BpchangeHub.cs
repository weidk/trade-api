using Microsoft.AspNetCore.SignalR;
using Newtonsoft.Json;
using RealtimeApi.Funcs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RealtimeApi.Hubs
{
    public class BpchangeHub : Hub
    {
        public override async Task OnConnectedAsync()
        {
            //string ToSendJson = JsonConvert.SerializeObject(Config.BpChangeDict, Formatting.Indented);
            var ToSendJson = MonitorBp.GetJsonString(Config.BpChangeDict);
            await Clients.Caller.SendAsync("ReceiveBp", ToSendJson);
        }
    }
}
