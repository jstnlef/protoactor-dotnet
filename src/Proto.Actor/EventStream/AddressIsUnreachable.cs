using System;

// ReSharper disable once CheckNamespace
namespace Proto;

public sealed record AddressIsUnreachableEvent(string Address);

#pragma warning disable RCS1194
public class AddressIsUnreachableException : Exception
#pragma warning restore RCS1194
{
    public AddressIsUnreachableException(string address) : base($"'{address}' is unreachable") {}
}
