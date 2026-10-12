using Broker.Logging;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Broker.Api;

public sealed class ExceptionInterceptor(IBrokerLog log) : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            return await continuation(request, context);
        }
        catch (Exception ex) when (IsUnexpected(ex, context))
        {
            throw Fail(ex, context);
        }
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            await continuation(request, responseStream, context);
        }
        catch (Exception ex) when (IsUnexpected(ex, context))
        {
            throw Fail(ex, context);
        }
    }

    private static bool IsUnexpected(Exception ex, ServerCallContext context) =>
        ex is not RpcException
        && !(ex is OperationCanceledException && context.CancellationToken.IsCancellationRequested);

    private RpcException Fail(Exception ex, ServerCallContext context)
    {
        log.Error($"{context.Method} from {context.Peer}: {ex}");
        return new RpcException(new Status(StatusCode.Internal, "INTERNAL_ERROR"));
    }
}
