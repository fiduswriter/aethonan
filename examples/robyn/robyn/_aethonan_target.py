# Vendored-port addition for the Aethonan example: a dummy RSGI target.
#
# Aethonan's worker bootstrap requires a Python RSGI application, but in this
# port every request is answered by the catch-all native route registered by
# the ported Robyn core (Server.start). This target is therefore never
# invoked; it only exists so the RSGI worker can start.
#
# BSD 2-Clause (c) 2021 Sanskar Jethi (Robyn); Aethonan port: see NOTICE.


class _NativeFallbackApp:
    async def __call__(self, scope, proto):
        # Reached only if a request escapes the catch-all native route.
        return proto.response_empty(404, [])


# The instance itself is the RSGI target: Aethonan calls it as app(scope, proto).
app = _NativeFallbackApp()
