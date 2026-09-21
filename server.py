"""
FINISTERRA MCP Server
=====================
MCP server providing Claude with direct access to the FINISTERRA project
excavation database at data.finisterra.icarehb.com.

Exposes tools for querying site data (XYZ coordinates, context/find info,
datums) for excavation sites: ESC (Escoural), GDC (Gruta da Companheira),
CARI (Carigüela).

Authentication via environment variables:
    FINISTERRA_USERNAME
    FINISTERRA_PASSWORD
"""

import os
import json
import time
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://data.finisterra.icarehb.com/api"

KNOWN_SITES = {
    "esc": "Escoural",
    "gdc": "Gruta da Companheira",
    "cari": "Carigüela",
}

KNOWN_TABLES = ["xyz", "context", "datums"]

# The upstream API ignores limit/offset/page query params and always returns the
# full table, so slicing has to happen here. Caching the fetched table means a
# paged read costs one HTTP fetch instead of one per page.
CACHE_TTL = float(os.environ.get("FINISTERRA_CACHE_TTL", "300"))

# Context codes that record excavation infrastructure rather than recovered
# material: photo markers, topographic shots and survey points. Filtering on the
# record code rather than the unit keeps genuine samples that happen to sit in a
# profile or section unit (CARI's corte1/corte2 hold OSL, ochre and lithics).
MARKER_CODES = {"PHOTO", "TOPOGRAPHY", "TOPO", "POINT"}

logger = logging.getLogger("finisterra-mcp")

# ---------------------------------------------------------------------------
# State management via lifespan
# ---------------------------------------------------------------------------

@dataclass
class AppState:
    """Holds the authenticated token, HTTP client, and fetched-table cache."""
    token: str | None = None
    client: httpx.AsyncClient | None = None
    # path -> (fetched_at_monotonic, rows)
    cache: dict[str, tuple[float, Any]] = field(default_factory=dict)


@asynccontextmanager
async def lifespan(server: FastMCP):
    """Startup/shutdown: create HTTP client and auto-authenticate if creds are set."""
    state = AppState()
    state.client = httpx.AsyncClient(timeout=60.0)

    username = os.environ.get("FINISTERRA_USERNAME")
    password = os.environ.get("FINISTERRA_PASSWORD")

    if username and password:
        try:
            resp = await state.client.post(
                f"{BASE_URL}/get-token/",
                data={"username": username, "password": password},
            )
            resp.raise_for_status()
            state.token = resp.json()["token"]
            logger.info("Auto-authenticated as %s", username)
        except Exception as e:
            logger.warning("Auto-authentication failed: %s", e)
    else:
        logger.info(
            "No FINISTERRA_USERNAME/FINISTERRA_PASSWORD set. "
            "Use the authenticate tool to connect."
        )

    try:
        yield state
    finally:
        await state.client.aclose()


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "finisterra",
    instructions=(
        "FINISTERRA excavation database server. Provides access to "
        "archaeological excavation data (XYZ coordinates, context/find "
        "records, datums) for sites: ESC (Escoural), GDC (Gruta da "
        "Companheira), CARI (Carigüela). Authenticate first if credentials "
        "were not provided via environment variables."
    ),
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

async def _authed_get(state: AppState, path: str) -> Any:
    """Make an authenticated GET request, return parsed JSON."""
    if not state.token:
        return {"error": "Not authenticated. Call the authenticate tool first."}
    if not state.client:
        return {"error": "HTTP client not initialised."}

    url = f"{BASE_URL}/{path.lstrip('/')}"
    resp = await state.client.get(
        url, headers={"Authorization": f"Token {state.token}"}
    )
    resp.raise_for_status()
    return resp.json()


async def _fetch_table(state: AppState, path: str, refresh: bool = False) -> Any:
    """Fetch a list endpoint, serving from cache when the entry is still fresh.

    The API has no pagination, so every call would otherwise re-download the
    whole table. Only list responses are cached; errors are passed straight
    through so they are never sticky.
    """
    now = time.monotonic()
    if not refresh and CACHE_TTL > 0:
        hit = state.cache.get(path)
        if hit is not None and (now - hit[0]) < CACHE_TTL:
            return hit[1]

    data = await _authed_get(state, path)
    if isinstance(data, list) and CACHE_TTL > 0:
        state.cache[path] = (now, data)
    return data


def _is_marker_code(code: Any) -> bool:
    """True if a context code records infrastructure rather than material."""
    return str(code or "").strip().upper() in MARKER_CODES


def _context_lookup(ctx_data: Any) -> dict[tuple, dict]:
    """Index context rows by their (squid, unit, idno) join key."""
    if not isinstance(ctx_data, list):
        return {}
    return {(r.get("squid"), r.get("unit"), r.get("idno")): r for r in ctx_data}


def _drop_markers(rows: list, ctx_lookup: dict[tuple, dict]) -> list:
    """Drop XYZ rows whose joined context record is a marker.

    Rows with no context match are kept: an unmatched row is a recording gap,
    not evidence that the row is infrastructure.
    """
    kept = []
    for r in rows:
        ctx = ctx_lookup.get((r.get("squid"), r.get("unit"), r.get("idno")))
        if ctx is not None and _is_marker_code(ctx.get("code")):
            continue
        kept.append(r)
    return kept


async def _xyz_without_markers(state: AppState, site: str, rows: list) -> tuple[list, int]:
    """Return (rows with markers dropped, number dropped)."""
    ctx_data = await _fetch_table(state, f"{site}/context/list/")
    kept = _drop_markers(rows, _context_lookup(ctx_data))
    return kept, len(rows) - len(kept)


def _paged_result(
    site: str,
    table: str,
    data: list,
    limit: int,
    offset: int,
    count_only: bool,
) -> dict:
    """Build a paginated response envelope over an already-fetched table."""
    total = len(data)
    if count_only:
        return {"site": site, "table": table, "total_records": total}

    offset = max(0, offset)
    subset = data[offset:] if limit == 0 else data[offset : offset + limit]
    next_offset = offset + len(subset)
    has_more = next_offset < total

    return {
        "site": site,
        "table": table,
        "total_records": total,
        "offset": offset,
        "returned": len(subset),
        "has_more": has_more,
        "next_offset": next_offset if has_more else None,
        "data": subset,
    }


def _validate_paging(limit: int, offset: int) -> str | None:
    """Return an error message for out-of-range paging args, else None."""
    if limit < 0:
        return f"Invalid limit {limit}. Use a positive number, or 0 for all records."
    if offset < 0:
        return f"Invalid offset {offset}. Offset must be 0 or greater."
    return None


def _truncated(data: list[dict], limit: int = 100) -> dict:
    """Return data with optional truncation info for large result sets."""
    total = len(data)
    if total <= limit:
        return {"total_records": total, "data": data}
    return {
        "total_records": total,
        "showing": limit,
        "note": f"Showing first {limit} of {total} records. Use offset/limit params for pagination.",
        "data": data[:limit],
    }


def _validate_site(site: str) -> str | None:
    """Return an error message if the site code is invalid, else None."""
    site = site.lower().strip()
    if site not in KNOWN_SITES:
        return (
            f"Unknown site '{site}'. "
            f"Available sites: {', '.join(f'{k} ({v})' for k, v in KNOWN_SITES.items())}"
        )
    return None


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
async def authenticate(username: str, password: str) -> str:
    """
    Authenticate with the FINISTERRA database.
    Only needed if FINISTERRA_USERNAME/FINISTERRA_PASSWORD env vars are not set.
    Returns confirmation on success.
    """
    state: AppState = mcp.get_context().request_context.lifespan_context
    try:
        resp = await state.client.post(
            f"{BASE_URL}/get-token/",
            data={"username": username, "password": password},
        )
        resp.raise_for_status()
        state.token = resp.json()["token"]
        # A new identity may see different rows; don't serve the old one's cache.
        state.cache.clear()
        return f"Authenticated successfully as {username}."
    except httpx.HTTPStatusError as e:
        return f"Authentication failed: HTTP {e.response.status_code}"
    except Exception as e:
        return f"Authentication failed: {e}"


@mcp.tool()
async def list_sites() -> str:
    """
    List all available excavation sites in the FINISTERRA database.
    Returns site codes and full names.
    """
    lines = ["Available FINISTERRA excavation sites:", ""]
    for code, name in KNOWN_SITES.items():
        lines.append(f"  {code} — {name}")
    lines.append("")
    lines.append("Each site has tables: xyz, context, datums")
    return "\n".join(lines)


@mcp.tool()
async def get_xyz(
    site: str,
    limit: int = 100,
    offset: int = 0,
    count_only: bool = False,
    exclude_markers: bool = False,
) -> str:
    """
    Get XYZ coordinate data for a site.

    Args:
        site: Site code — 'esc', 'gdc', or 'cari'
        limit: Max records to return (default 100, use 0 for all)
        offset: Number of records to skip for pagination
        count_only: Return just the record count, no data. Cheap way to size a
            table before pulling it.
        exclude_markers: Drop records coded PHOTO/TOPOGRAPHY/POINT, which record
            excavation infrastructure rather than recovered material.
    """
    state: AppState = mcp.get_context().request_context.lifespan_context
    err = _validate_site(site) or _validate_paging(limit, offset)
    if err:
        return err

    try:
        data = await _fetch_table(state, f"{site.lower()}/xyz/list/")
        if isinstance(data, dict) and "error" in data:
            return data["error"]
        if not isinstance(data, list):
            return json.dumps(data, indent=2)

        dropped = 0
        if exclude_markers:
            data, dropped = await _xyz_without_markers(state, site.lower(), data)

        result = _paged_result(site.lower(), "xyz", data, limit, offset, count_only)
        if exclude_markers:
            result["markers_excluded"] = dropped
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return f"HTTP error {e.response.status_code}: {e.response.text}"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_context(
    site: str,
    limit: int = 100,
    offset: int = 0,
    count_only: bool = False,
    exclude_markers: bool = False,
) -> str:
    """
    Get context/find data for a site.

    Args:
        site: Site code — 'esc', 'gdc', or 'cari'
        limit: Max records to return (default 100, use 0 for all)
        offset: Number of records to skip for pagination
        count_only: Return just the record count, no data. Cheap way to size a
            table before pulling it.
        exclude_markers: Drop records coded PHOTO/TOPOGRAPHY/POINT, which record
            excavation infrastructure rather than recovered material.
    """
    state: AppState = mcp.get_context().request_context.lifespan_context
    err = _validate_site(site) or _validate_paging(limit, offset)
    if err:
        return err

    try:
        data = await _fetch_table(state, f"{site.lower()}/context/list/")
        if isinstance(data, dict) and "error" in data:
            return data["error"]
        if not isinstance(data, list):
            return json.dumps(data, indent=2)

        dropped = 0
        if exclude_markers:
            before = len(data)
            data = [r for r in data if not _is_marker_code(r.get("code"))]
            dropped = before - len(data)

        result = _paged_result(site.lower(), "context", data, limit, offset, count_only)
        if exclude_markers:
            result["markers_excluded"] = dropped
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return f"HTTP error {e.response.status_code}: {e.response.text}"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_datums(site: str) -> str:
    """
    Get datum points for a site.

    Args:
        site: Site code — 'esc', 'gdc', or 'cari'
    """
    state: AppState = mcp.get_context().request_context.lifespan_context
    err = _validate_site(site)
    if err:
        return err

    try:
        data = await _fetch_table(state, f"{site.lower()}/datums/list/")
        if isinstance(data, dict) and "error" in data:
            return data["error"]

        result = {
            "site": site.lower(),
            "table": "datums",
            "total_records": len(data) if isinstance(data, list) else 1,
            "data": data,
        }
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return f"HTTP error {e.response.status_code}: {e.response.text}"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_site_data(
    site: str,
    limit: int = 100,
    offset: int = 0,
    count_only: bool = False,
    exclude_markers: bool = False,
) -> str:
    """
    Get combined XYZ + Context data for a site (left join on squid, unit, idno).
    This mirrors the finisterraR::get_site_data() function.

    Args:
        site: Site code — 'esc', 'gdc', or 'cari'
        limit: Max records to return (default 100, use 0 for all)
        offset: Number of records to skip for pagination
        count_only: Return row counts and join coverage only, no data. The full
            join is far too large to return in one response, so use this first
            to size the pull.
        exclude_markers: Drop records coded PHOTO/TOPOGRAPHY/POINT, which record
            excavation infrastructure rather than recovered material.
    """
    state: AppState = mcp.get_context().request_context.lifespan_context
    err = _validate_site(site) or _validate_paging(limit, offset)
    if err:
        return err

    try:
        xyz_data = await _fetch_table(state, f"{site.lower()}/xyz/list/")
        if isinstance(xyz_data, dict) and "error" in xyz_data:
            return xyz_data["error"]

        ctx_data = await _fetch_table(state, f"{site.lower()}/context/list/")
        if isinstance(ctx_data, dict) and "error" in ctx_data:
            return ctx_data["error"]

        # Build a lookup from context data keyed by (squid, unit, idno)
        ctx_lookup: dict[tuple, dict] = {}
        if isinstance(ctx_data, list):
            for row in ctx_data:
                key = (row.get("squid"), row.get("unit"), row.get("idno"))
                ctx_lookup[key] = row

        rows = xyz_data if isinstance(xyz_data, list) else []
        dropped = 0
        if exclude_markers:
            before = len(rows)
            rows = _drop_markers(rows, ctx_lookup)
            dropped = before - len(rows)

        # Left join: xyz as base, merge context fields
        combined = []
        matched = 0
        for row in rows:
            key = (row.get("squid"), row.get("unit"), row.get("idno"))
            merged = {**row}
            if key in ctx_lookup:
                matched += 1
                for k, v in ctx_lookup[key].items():
                    if k not in merged:
                        merged[k] = v
            combined.append(merged)

        result = _paged_result(
            site.lower(), "xyz + context (joined)", combined, limit, offset, count_only
        )
        result["xyz_records"] = len(rows)
        result["context_records"] = len(ctx_data) if isinstance(ctx_data, list) else 0
        result["joined_with_context"] = matched
        result["missing_context"] = len(combined) - matched
        if exclude_markers:
            result["markers_excluded"] = dropped
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return f"HTTP error {e.response.status_code}: {e.response.text}"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_table_schema(site: str, table: str) -> str:
    """
    Fetch a small sample from a table and report its field names and types.
    Useful for understanding the data structure before querying.

    Args:
        site: Site code — 'esc', 'gdc', or 'cari'
        table: Table name — 'xyz', 'context', or 'datums'
    """
    state: AppState = mcp.get_context().request_context.lifespan_context
    err = _validate_site(site)
    if err:
        return err

    table = table.lower().strip()
    if table not in KNOWN_TABLES:
        return f"Unknown table '{table}'. Available tables: {', '.join(KNOWN_TABLES)}"

    try:
        data = await _fetch_table(state, f"{site.lower()}/{table}/list/")
        if isinstance(data, dict) and "error" in data:
            return data["error"]
        if not isinstance(data, list) or len(data) == 0:
            return f"No data found in {site}/{table}."

        sample = data[0]
        schema = {}
        for k, v in sample.items():
            schema[k] = type(v).__name__ if v is not None else "null"

        result = {
            "site": site.lower(),
            "table": table,
            "total_records": len(data),
            "fields": schema,
            "sample_record": sample,
        }
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return f"HTTP error {e.response.status_code}: {e.response.text}"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def search_by_square(site: str, square_id: str) -> str:
    """
    Search for all XYZ records from a specific excavation square.

    Args:
        site: Site code — 'esc', 'gdc', or 'cari'
        square_id: The square identifier to filter by (squid field)
    """
    state: AppState = mcp.get_context().request_context.lifespan_context
    err = _validate_site(site)
    if err:
        return err

    try:
        data = await _fetch_table(state, f"{site.lower()}/xyz/list/")
        if isinstance(data, dict) and "error" in data:
            return data["error"]
        if not isinstance(data, list):
            return json.dumps(data, indent=2)

        matches = [r for r in data if str(r.get("squid", "")).lower() == square_id.lower()]

        result = {
            "site": site.lower(),
            "square": square_id,
            "total_matches": len(matches),
            "data": matches,
        }
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return f"HTTP error {e.response.status_code}: {e.response.text}"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def list_squares(
    site: str,
    unit: str | None = None,
    limit: int = 500,
    offset: int = 0,
    count_only: bool = False,
    exclude_markers: bool = False,
) -> str:
    """
    List the distinct excavation squares (squid values) recorded for a site.

    summary_stats reports how many squares a site has but not which ones, because
    inlining several thousand ids overflows the response limit. Use this to get
    the ids themselves, a page at a time.

    Args:
        site: Site code — 'esc', 'gdc', or 'cari'
        unit: Optional unit code to filter by (e.g. 'N19'), case-insensitive
        limit: Max squares to return (default 500, use 0 for all)
        offset: Number of squares to skip for pagination
        count_only: Return just the number of distinct squares, no ids
        exclude_markers: Ignore records coded PHOTO/TOPOGRAPHY/POINT when
            collecting squares. Squares recorded only by a photo or survey shot
            drop out entirely, which removes the pure-marker profile units.

    Note: total_records counts distinct squares here, not XYZ rows.
    """
    state: AppState = mcp.get_context().request_context.lifespan_context
    err = _validate_site(site) or _validate_paging(limit, offset)
    if err:
        return err

    try:
        data = await _fetch_table(state, f"{site.lower()}/xyz/list/")
        if isinstance(data, dict) and "error" in data:
            return data["error"]
        if not isinstance(data, list):
            return json.dumps(data, indent=2)

        rows = data
        dropped = 0
        if exclude_markers:
            rows, dropped = await _xyz_without_markers(state, site.lower(), rows)
            data = rows
        if unit:
            wanted = unit.strip().lower()
            rows = [r for r in data if str(r.get("unit", "")).lower() == wanted]
            if not rows:
                known = sorted({str(r.get("unit")) for r in data if r.get("unit")})
                return (
                    f"No squares found for unit '{unit}' at {site.lower()}. "
                    f"Known units: {', '.join(known)}"
                )

        squares = sorted({str(r.get("squid")) for r in rows if r.get("squid")})

        result = _paged_result(site.lower(), "squares", squares, limit, offset, count_only)
        if "data" in result:
            result["squares"] = result.pop("data")
        if unit:
            result["unit_filter"] = unit
        if exclude_markers:
            result["marker_records_ignored"] = dropped
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return f"HTTP error {e.response.status_code}: {e.response.text}"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def summary_stats(site: str, exclude_markers: bool = False) -> str:
    """
    Get a quick summary of a site's data: record counts, units, coordinate
    ranges, etc. Use list_squares for the square ids themselves.

    Args:
        site: Site code — 'esc', 'gdc', or 'cari'
        exclude_markers: Drop records coded PHOTO/TOPOGRAPHY/POINT, which record
            excavation infrastructure rather than recovered material. Units that
            contain nothing else (profile and survey-point units) disappear from
            the summary entirely.
    """
    state: AppState = mcp.get_context().request_context.lifespan_context
    err = _validate_site(site)
    if err:
        return err

    try:
        xyz_data = await _fetch_table(state, f"{site.lower()}/xyz/list/")
        ctx_data = await _fetch_table(state, f"{site.lower()}/context/list/")
        datum_data = await _fetch_table(state, f"{site.lower()}/datums/list/")

        summary: dict[str, Any] = {
            "site": site.lower(),
            "site_name": KNOWN_SITES.get(site.lower(), "Unknown"),
        }

        if exclude_markers:
            n_xyz = len(xyz_data) if isinstance(xyz_data, list) else 0
            n_ctx = len(ctx_data) if isinstance(ctx_data, list) else 0
            if isinstance(xyz_data, list):
                xyz_data = _drop_markers(xyz_data, _context_lookup(ctx_data))
            if isinstance(ctx_data, list):
                ctx_data = [r for r in ctx_data if not _is_marker_code(r.get("code"))]
            summary["exclude_markers"] = True
            summary["markers_excluded"] = {
                "xyz": n_xyz - (len(xyz_data) if isinstance(xyz_data, list) else 0),
                "context": n_ctx - (len(ctx_data) if isinstance(ctx_data, list) else 0),
                "codes": sorted(MARKER_CODES),
            }

        # XYZ stats. The square ids are deliberately not inlined: sites have
        # thousands of them, which overflows the response limit and made this
        # tool unusable for ESC and CARI. Use list_squares for the ids.
        if isinstance(xyz_data, list) and xyz_data:
            squares = {str(r.get("squid", "")) for r in xyz_data if r.get("squid")}
            units = sorted({str(r.get("unit", "")) for r in xyz_data if r.get("unit")})

            xs = [r["x"] for r in xyz_data if r.get("x") is not None]
            ys = [r["y"] for r in xyz_data if r.get("y") is not None]
            zs = [r["z"] for r in xyz_data if r.get("z") is not None]

            summary["xyz"] = {
                "total_records": len(xyz_data),
                "n_squares": len(squares),
                "units": units,
                "n_units": len(units),
                "squares_hint": (
                    f"Use list_squares('{site.lower()}'"
                    + (", exclude_markers=True" if exclude_markers else "")
                    + f") for the {len(squares)} square ids."
                ),
            }
            if xs:
                summary["xyz"]["x_range"] = [min(xs), max(xs)]
            if ys:
                summary["xyz"]["y_range"] = [min(ys), max(ys)]
            if zs:
                summary["xyz"]["z_range"] = [min(zs), max(zs)]
        else:
            summary["xyz"] = {"total_records": 0}

        # Context stats
        if isinstance(ctx_data, list):
            summary["context"] = {"total_records": len(ctx_data)}
        else:
            summary["context"] = {"total_records": 0}

        # Datums stats: names only; get_datums returns the coordinates.
        if isinstance(datum_data, list):
            summary["datums"] = {
                "total_records": len(datum_data),
                "names": [str(d.get("name")) for d in datum_data if d.get("name")],
                "data_hint": f"Use get_datums('{site.lower()}') for datum coordinates.",
            }
        else:
            summary["datums"] = {"total_records": 0}

        return json.dumps(summary, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return f"HTTP error {e.response.status_code}: {e.response.text}"
    except Exception as e:
        return f"Error: {e}"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run(transport="stdio")
