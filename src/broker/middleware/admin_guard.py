"""
Admin tunnel guard.

The broker's ``/admin/*`` endpoints (API-key CRUD, connect-token minting) are
only ever used by the operator locally — over ``ssh -L 8002:localhost:8002`` on
the Tailscale admin plane. They must never be reachable from the public internet.

The broker binds ``127.0.0.1`` only, but the Cloudflare Tunnel (cloudflared, on
the same host) forwards every path from the public hostname to ``localhost:8002``
— re-exposing ``/admin`` publicly behind only the admin key. This middleware
closes that gap: any ``/admin`` request that arrived through Cloudflare is
answered ``404``, indistinguishable from a path that does not exist.

Tunnel traffic is identified by the ``Cf-Ray`` / ``Cf-Connecting-Ip`` headers
Cloudflare injects at its edge on every proxied request. A client cannot forge or
strip them (Cloudflare overwrites them), and the only way to reach the broker
*without* them is locally — ``ssh -L`` to ``127.0.0.1`` or the tailnet, both of
which are the trusted admin plane. Those requests carry no ``Cf-*`` headers and
pass through untouched.

This is defense-in-depth behind the recommended edge rule (a Cloudflare WAF /
Access block on ``bobsmcp.uk/admin/*``): even if that edge rule is ever removed
or misconfigured, the origin still refuses tunnelled admin traffic.
"""

from __future__ import annotations

import json

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

# Headers Cloudflare sets on every request it proxies through the tunnel. Their
# presence reliably marks traffic that entered via the public hostname — a client
# cannot remove them (CF overwrites at its edge), and local/tailnet requests to
# 127.0.0.1 never carry them.
_CLOUDFLARE_EDGE_HEADERS = ("cf-ray", "cf-connecting-ip")


class AdminTunnelGuardMiddleware(BaseHTTPMiddleware):
    """404s ``/admin/*`` requests that arrived through the Cloudflare tunnel."""

    def __init__(self, app: object, guarded_prefix: str = "/admin") -> None:
        super().__init__(app)  # type: ignore[arg-type]
        self._guarded_prefix = guarded_prefix

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.url.path.startswith(self._guarded_prefix) and _came_via_tunnel(request):
            # 404 (not 403): a public prober learns nothing about the admin
            # surface — it looks exactly like a path that does not exist.
            return Response(
                status_code=404,
                content=json.dumps({"error": "Not Found"}),
                media_type="application/json",
            )
        return await call_next(request)


def _came_via_tunnel(request: Request) -> bool:
    """True if the request carries any Cloudflare edge header (case-insensitive)."""
    return any(header in request.headers for header in _CLOUDFLARE_EDGE_HEADERS)
