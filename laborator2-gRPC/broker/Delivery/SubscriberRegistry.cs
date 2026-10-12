using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace Broker.Delivery;

public sealed class SubscriberRegistry
{
    private readonly ConcurrentDictionary<string, SubscriberSession> _sessions = new();

    public IReadOnlyList<string> OnlineSubscriberIds => _sessions.Keys.ToList();

    public SubscriberSession Open(string subscriberId, string peer)
    {
        var session = new SubscriberSession(subscriberId, peer);
        while (true)
        {
            if (_sessions.TryGetValue(subscriberId, out SubscriberSession? existing))
            {
                if (_sessions.TryUpdate(subscriberId, session, existing))
                {
                    existing.MarkReplaced();
                    return session;
                }
            }
            else if (_sessions.TryAdd(subscriberId, session))
            {
                return session;
            }
        }
    }

    public void Close(SubscriberSession session)
    {
        _sessions.TryRemove(new KeyValuePair<string, SubscriberSession>(session.SubscriberId, session));
        session.Close();
    }

    public void CloseAll()
    {
        foreach (string id in _sessions.Keys)
        {
            if (_sessions.TryRemove(id, out SubscriberSession? session))
                session.Close();
        }
    }

    public bool TryGet(string subscriberId, [MaybeNullWhen(false)] out SubscriberSession session) =>
        _sessions.TryGetValue(subscriberId, out session);

    public bool IsOnline(string subscriberId) => _sessions.ContainsKey(subscriberId);
}
