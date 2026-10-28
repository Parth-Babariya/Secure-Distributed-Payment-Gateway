"""gRPC server interceptors: authentication, peer identity and access logging."""
import logging
import time

import grpc

from auth import TokenError
from tls import peer_common_name

log = logging.getLogger("access")


def _abort_handler(code, details):
    """A handler that rejects the call with the given status."""
    def reject(request, context):
        context.abort(code, details)
    return grpc.unary_unary_rpc_method_handler(reject)


def _wrap_unary(handler, wrapper_factory):
    """Return a copy of a unary-unary handler whose behaviour is wrapped."""
    if handler is None or handler.unary_unary is None:
        return handler
    return grpc.unary_unary_rpc_method_handler(
        wrapper_factory(handler.unary_unary),
        request_deserializer=handler.request_deserializer,
        response_serializer=handler.response_serializer,
    )


def _method_name(full_method):
    return full_method.rsplit("/", 1)[-1]


class AuthorizationInterceptor(grpc.ServerInterceptor):
    """Requires a valid, unrevoked JWT on every method except the exempt ones."""

    def __init__(self, tokens, exempt_methods):
        self.tokens = tokens
        self.exempt_methods = set(exempt_methods)

    def intercept_service(self, continuation, handler_call_details):
        if _method_name(handler_call_details.method) in self.exempt_methods:
            return continuation(handler_call_details)
        metadata = dict(handler_call_details.invocation_metadata or ())
        try:
            self.tokens.verify(metadata.get("authorization"))
        except TokenError as e:
            log.warning("Rejected %s: %s", handler_call_details.method, e)
            return _abort_handler(grpc.StatusCode.UNAUTHENTICATED, str(e))
        return continuation(handler_call_details)


class PeerIdentityInterceptor(grpc.ServerInterceptor):
    """Only lets callers whose mTLS certificate CN is in `allowed` through.

    Methods in `exempt_methods` skip this check and must authorise the caller
    themselves (the gateway's RegisterBank checks for the bank's own CN).
    """

    def __init__(self, allowed, exempt_methods=()):
        self.allowed = set(allowed)
        self.exempt_methods = set(exempt_methods)

    def intercept_service(self, continuation, handler_call_details):
        if _method_name(handler_call_details.method) in self.exempt_methods:
            return continuation(handler_call_details)
        allowed = self.allowed

        def factory(behaviour):
            def checked(request, context):
                peer = peer_common_name(context)
                if peer not in allowed:
                    log.warning("Rejected %s from certificate %r", handler_call_details.method, peer)
                    context.abort(grpc.StatusCode.PERMISSION_DENIED, "Caller certificate not authorised")
                return behaviour(request, context)
            return checked

        return _wrap_unary(continuation(handler_call_details), factory)


class LoggingInterceptor(grpc.ServerInterceptor):
    """Logs every call with its final status code and latency."""

    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method

        def factory(behaviour):
            def logged(request, context):
                start = time.perf_counter()
                try:
                    return behaviour(request, context)
                finally:
                    code = context.code() or grpc.StatusCode.OK
                    elapsed_ms = (time.perf_counter() - start) * 1000
                    log.info("%s %s %.1fms peer=%s", method, code.name, elapsed_ms, context.peer())
            return logged

        return _wrap_unary(continuation(handler_call_details), factory)
