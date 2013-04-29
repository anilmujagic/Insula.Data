using System;
using System.Runtime.Serialization;

namespace Insula.Data.Orm
{
    [Serializable]
    public class SqlStatementException : Exception
    {
        public SqlStatementException()
        {
        }

        public SqlStatementException(string message)
            : base(message)
        {
        }

        public SqlStatementException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        protected SqlStatementException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
