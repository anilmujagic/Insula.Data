using System;

namespace Insula.DataAnnotations.Schema
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public sealed class IdentityAttribute : Attribute
    {
    }
}
