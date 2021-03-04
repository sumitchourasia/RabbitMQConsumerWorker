using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RabbitMQAPI.RequestResponseLog
{
        public class RequestResponse 
        {
                public string APIName { get; set; }
                public string ProgramCode { get; set; }
                public dynamic Request { get; set; }
                public dynamic Response { get; set; }
                public string ApplicationId { get; set; }
                public string ApplicationName { get; set; }
                public string IpAddress { get; set; }
                public string Createdby { get; set; }
                public string Month { get; set; }
                public string Date { get; set; }
                public string ProductName { get; set; }
                public string SessionId { get; set; }
                public bool IsRequest { get; set; }
                public bool IsExcepton { get; set; } = false;
                public long TimeStamp { get; set; }
        }
}
