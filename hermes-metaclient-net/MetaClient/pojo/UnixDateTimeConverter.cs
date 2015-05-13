using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json;

namespace com.ctrip.hermes.meta.client.pojo
{
    public class UnixDateTimeConverter : DateTimeConverterBase
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            long val;
            if (value is DateTime)
            {
                val = UnixDateTimeHelper.ToUnixTime((DateTime)value)*1000;
            }
            else
            {
                throw new Exception("Expected date object value.");
            }
            writer.WriteValue(val);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
                                       JsonSerializer serializer)
        {
            if (reader.Value == null)
                return null;
            if (reader.TokenType != JsonToken.Integer)
                throw new Exception("Wrong Token Type");

            long ticks = (long)reader.Value;
            return UnixDateTimeHelper.FromUnixTime(ticks/1000);
        }
    }

    public static class UnixDateTimeHelper
    {
        private const string InvalidUnixEpochErrorMessage = "Unix epoc starts January 1st, 1970";
        /// <summary>
        ///   Convert a long into a DateTime
        /// </summary>
        public static DateTime FromUnixTime(this Int64 self)
        {
            var ret = new DateTime(1970, 1, 1);
            return ret.AddSeconds(self);
        }

        /// <summary>
        ///   Convert a DateTime into a long
        /// </summary>
        public static Int64 ToUnixTime(this DateTime self)
        {

            if (self == DateTime.MinValue)
            {
                return 0;
            }

            var epoc = new DateTime(1970, 1, 1);
            var delta = self - epoc;

            if (delta.TotalSeconds < 0) throw new ArgumentOutOfRangeException(InvalidUnixEpochErrorMessage);

            return (long)delta.TotalSeconds;
        }
    }
}
