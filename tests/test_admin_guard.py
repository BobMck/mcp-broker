"""Admin tunnel guard — /admin blocked when it arrives via Cloudflare, else open."""

from __future__ import annotations

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from broker.middleware.admin_guard import AdminTunnelGuardMiddleware


def _client() -> TestClient:
    async def admin(request):  # noqa: ANN001, ANN202 -- test stub
        return PlainTextResponse("admin-ok")

    async def proxy(request):  # noqa: ANN001, ANN202 -- test stub
        return PlainTextResponse("proxy-ok")

    app = Starlette(
        routes=[
            Route("/admin/keys", admin, methods=["GET", "POST"]),
            Route("/proxy/x", proxy),
        ]
    )
    app.add_middleware(AdminTunnelGuardMiddleware)
    return TestClient(app)


def test_admin_via_cf_ray_is_404() -> None:
    r = _client().get("/admin/keys", headers={"cf-ray": "8f3ab12cd-LHR"})
    assert r.status_code == 404


def test_admin_via_cf_connecting_ip_is_404() -> None:
    r = _client().post("/admin/keys", headers={"cf-connecting-ip": "203.0.113.7"})
    assert r.status_code == 404


def test_admin_local_request_passes_through() -> None:
    # No Cf-* headers = local ssh -L / tailnet request.
    r = _client().get("/admin/keys")
    assert r.status_code == 200
    assert r.text == "admin-ok"


def test_non_admin_via_tunnel_is_untouched() -> None:
    # A tunnelled /proxy request is normal public traffic — must not be blocked.
    r = _client().get("/proxy/x", headers={"cf-ray": "8f3ab12cd-LHR"})
    assert r.status_code == 200
    assert r.text == "proxy-ok"
