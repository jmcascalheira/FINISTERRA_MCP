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

Writes (add_record, update_record) are disabled unless FINISTERRA_ALLOW_WRITES=1.
"""

import os
import json
import time
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

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

# Placeholder strings stored where a null was meant. They are real values as far
# as the database is concerned, so field_summary reports them separately rather
# than folding them into the blank count.
NULL_LIKE = {"NA", "N/A", "NONE", "NULL", "-", "--", "?", "N.A."}

# Writes are off unless explicitly enabled, so a read-only setup stays read-only.
ALLOW_WRITES = os.environ.get("FINISTERRA_ALLOW_WRITES", "").strip().lower() in {
    "1", "true", "yes", "on",
}

# Writable fields per table, from the backend models (finisterra/models.py).
# "id" is server-assigned and never sent.
WRITABLE_FIELDS = {
    "context": {
        "squid", "unit", "idno", "sitename", "code", "excavator", "level",
        "spit", "feature", "date", "year", "notes",
    },
    "xyz": {"squid", "unit", "idno", "suffix", "prism", "x", "y", "z", "notes"},
    "datums": {"name", "x", "y", "z", "date", "notes"},
}

# Natural key of each table. Context's key is also its primary key; XYZ and
# datums are addressed by an auto id that we resolve from the key. Key fields
# can't be changed by update_record: changing a primary key through the API
# inserts a new row rather than renaming the old one.
KEY_FIELDS = {
    "context": ("squid",),
    "xyz": ("squid", "suffix"),
    "datums": ("name",),
}

REQUIRED_ON_CREATE = {
    "context": ("squid",),
    "xyz": ("squid", "suffix"),
    "datums": ("name", "x", "y", "z"),
}

# Upstream bug: cari_XYZSerializer and cari_DatumsSerializer in the backend's
# api/serializers.py are bound to the ESC models, so a create would land in the
# ESC database and an update validates against ESC rows. Refuse until fixed.
BLOCKED_WRITES = {
    ("cari", "xyz"): "the backend's cari_XYZSerializer is bound to the ESC XYZ model",
    ("cari", "datums"): "the backend's cari_DatumsSerializer is bound to the ESC Datums model",
}

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
        "were not provided via environment variables. add_record and "
        "update_record write to the live database: they default to a dry run, "
        "so show the user the preview and only pass dry_run=False once they "
        "have confirmed it."
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


async def _authed_send(state: AppState, method: str, path: str, payload: dict) -> Any:
    """Make an authenticated write request. Returns parsed JSON, or an error dict
    carrying the server's validation message on a 4xx."""
    if not state.token:
        return {"error": "Not authenticated. Call the authenticate tool first."}
    if not state.client:
        return {"error": "HTTP client not initialised."}

    url = f"{BASE_URL}/{path.lstrip('/')}"
    resp = await state.client.request(
        method, url, json=payload, headers={"Authorization": f"Token {state.token}"}
    )
    if 400 <= resp.status_code < 500:
        try:
            detail = resp.json()
        except ValueError:
            detail = resp.text
        return {"error": f"HTTP {resp.status_code} from {method} {path}", "detail": detail}
    resp.raise_for_status()
    return resp.json()


def _invalidate_site(state: AppState, site: str) -> None:
    """Drop every cached table for a site. A context write changes the XYZ join
    and marker filtering too, so clearing per-table isn't enough."""
    for path in [p for p in state.cache if p.startswith(f"{site}/")]:
        del state.cache[path]


def _key_matches(row: dict, table: str, key: dict) -> bool:
    """True if a row's natural-key fields equal the given key (string compare,
    so suffix 1 and "1" match)."""
    return all(str(row.get(k)) == str(key[k]) for k in KEY_FIELDS[table])


def _validate_write(site: str, table: str) -> str | None:
    """Return an error message if writes to site/table aren't allowed, else None."""
    if not ALLOW_WRITES:
        return (
            "Writes are disabled. Set FINISTERRA_ALLOW_WRITES=1 in the server's "
            "environment to enable add_record and update_record."
        )
    err = _validate_site(site)
    if err:
        return err
    if table not in WRITABLE_FIELDS:
        return f"Unknown table '{table}'. Writable tables: {', '.join(WRITABLE_FIELDS)}"
    blocked = BLOCKED_WRITES.get((site, table))
    if blocked:
        return (
            f"Writes to {site}/{table} are blocked: {blocked}, so the write would "
            "hit the wrong site. Fix the serializer upstream first."
        )
    return None


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
async def field_summary(
    site: str,
    table: str,
    field: str,
    top: int = 50,
    exclude_markers: bool = False,
) -> str:
    """
    Count the distinct values of any field in a table.

    Answers "how many X are there" for fields the other tools do not expose —
    level, spit, feature, code, excavator, year and so on — without pulling the
    table into the conversation.

    Args:
        site: Site code — 'esc', 'gdc', or 'cari'
        table: Table name — 'xyz', 'context', or 'datums'
        field: Field to summarise. An unknown name returns the available fields.
        top: Max distinct values to return, most frequent first (default 50,
            use 0 for all). High-cardinality fields like squid will be truncated.
        exclude_markers: Ignore PHOTO/TOPOGRAPHY/POINT records. Ignored for the
            datums table, which has no context codes.

    Numeric fields also get min/max/mean, since a list of distinct coordinates
    is rarely what you want.
    """
    state: AppState = mcp.get_context().request_context.lifespan_context
    err = _validate_site(site)
    if err:
        return err

    table = table.lower().strip()
    if table not in KNOWN_TABLES:
        return f"Unknown table '{table}'. Available tables: {', '.join(KNOWN_TABLES)}"
    if top < 0:
        return f"Invalid top {top}. Use a positive number, or 0 for all values."

    try:
        data = await _fetch_table(state, f"{site.lower()}/{table}/list/")
        if isinstance(data, dict) and "error" in data:
            return data["error"]
        if not isinstance(data, list) or not data:
            return f"No data found in {site.lower()}/{table}."

        available = sorted({k for row in data if isinstance(row, dict) for k in row})
        if field not in available:
            return (
                f"Unknown field '{field}' in {site.lower()}/{table}. "
                f"Available fields: {', '.join(available)}"
            )

        notes = []
        dropped = 0
        if exclude_markers:
            if table == "datums":
                notes.append("exclude_markers ignored: the datums table has no context codes.")
            elif table == "context":
                before = len(data)
                data = [r for r in data if not _is_marker_code(r.get("code"))]
                dropped = before - len(data)
            else:
                data, dropped = await _xyz_without_markers(state, site.lower(), data)

        total = len(data)
        counts: dict[str, int] = {}
        blank = 0
        numeric_vals: list[float] = []
        all_numeric = True

        for row in data:
            raw = row.get(field)
            if raw is None or str(raw).strip() == "":
                blank += 1
                continue
            key = str(raw).strip()
            counts[key] = counts.get(key, 0) + 1
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                all_numeric = False
            else:
                numeric_vals.append(float(raw))

        ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        shown = ordered if top == 0 else ordered[:top]

        result: dict[str, Any] = {
            "site": site.lower(),
            "table": table,
            "field": field,
            "total_records": total,
            "populated": total - blank,
            "blank": blank,
            "distinct_values": len(counts),
            "returned": len(shown),
            "truncated": len(shown) < len(counts),
            "values": [{"value": v, "count": n} for v, n in shown],
        }

        null_like = {v: n for v, n in ordered if v.upper() in NULL_LIKE}
        if null_like:
            result["null_like"] = null_like
            result["null_like_note"] = (
                "These are placeholder strings, not nulls. They are counted as "
                "populated values above; treat them as missing data."
            )

        if all_numeric and numeric_vals:
            result["numeric"] = {
                "min": min(numeric_vals),
                "max": max(numeric_vals),
                "mean": round(sum(numeric_vals) / len(numeric_vals), 3),
            }

        if exclude_markers and table != "datums":
            result["markers_excluded"] = dropped
        if notes:
            result["notes"] = notes
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
# Write tools
# ---------------------------------------------------------------------------

@mcp.tool()
async def add_record(
    site: str,
    table: str,
    record: dict[str, Any],
    dry_run: bool = True,
) -> str:
    """
    Add a new record to a site's context, xyz or datums table.

    Disabled unless the server runs with FINISTERRA_ALLOW_WRITES=1. Defaults to a
    dry run: it validates the record and shows exactly what would be sent. Call
    again with dry_run=False to write. There is no delete, so check the dry run.

    Args:
        site: Site code — 'esc', 'gdc', or 'cari'
        table: 'context', 'xyz', or 'datums'
        record: Field values. Required: context → squid; xyz → squid, suffix;
            datums → name, x, y, z. An xyz record's squid must already exist in
            the context table; its unit/idno are copied from that context
            record when omitted.
        dry_run: Validate and preview without writing (default True)
    """
    state: AppState = mcp.get_context().request_context.lifespan_context
    site, table = site.lower().strip(), table.lower().strip()
    err = _validate_write(site, table)
    if err:
        return err

    unknown = sorted(set(record) - WRITABLE_FIELDS[table])
    if unknown:
        return (
            f"Unknown field(s) for {table}: {', '.join(unknown)}. "
            f"Writable fields: {', '.join(sorted(WRITABLE_FIELDS[table]))}"
        )
    missing = [f for f in REQUIRED_ON_CREATE[table] if record.get(f) in (None, "")]
    if missing:
        return f"Missing required field(s) for {table}: {', '.join(missing)}"

    try:
        # Fresh reads: a duplicate check against a 5-minute-old cache isn't one.
        rows = await _fetch_table(state, f"{site}/{table}/list/", refresh=True)
        if isinstance(rows, dict) and "error" in rows:
            return rows["error"]

        key = {k: record[k] for k in KEY_FIELDS[table]}
        existing = [r for r in rows if _key_matches(r, table, key)]
        if existing:
            return json.dumps({
                "error": f"A {table} record with {key} already exists. Use update_record to change it.",
                "existing": existing[0],
            }, indent=2, default=str)

        payload = dict(record)
        warnings = []
        if table == "xyz":
            ctx_rows = await _fetch_table(state, f"{site}/context/list/", refresh=True)
            if isinstance(ctx_rows, dict) and "error" in ctx_rows:
                return ctx_rows["error"]
            ctx = next((r for r in ctx_rows if str(r.get("squid")) == str(record["squid"])), None)
            if ctx is None:
                return (
                    f"No context record with squid '{record['squid']}' at {site}. "
                    "XYZ rows reference a context record; add that first."
                )
            for f in ("unit", "idno"):
                if payload.get(f) in (None, ""):
                    payload[f] = ctx.get(f)
                elif str(payload[f]) != str(ctx.get(f)):
                    warnings.append(
                        f"{f} '{payload[f]}' differs from the context record's '{ctx.get(f)}'."
                    )

        path = f"{site}/{table}/create/"
        result: dict[str, Any] = {"site": site, "table": table, "method": "POST", "path": path}
        if warnings:
            result["warnings"] = warnings

        if dry_run:
            result["dry_run"] = True
            result["payload"] = payload
            result["note"] = "Nothing written. Call again with dry_run=False to create this record."
            return json.dumps(result, indent=2, default=str)

        created = await _authed_send(state, "POST", path, payload)
        if isinstance(created, dict) and "error" in created:
            return json.dumps(created, indent=2, default=str)
        _invalidate_site(state, site)
        result["created"] = created
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return f"HTTP error {e.response.status_code}: {e.response.text}"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def update_record(
    site: str,
    table: str,
    key: dict[str, Any],
    fields: dict[str, Any],
    dry_run: bool = True,
) -> str:
    """
    Change fields on one existing record in a site's context, xyz or datums table.

    Disabled unless the server runs with FINISTERRA_ALLOW_WRITES=1. Defaults to a
    dry run that shows the current values next to the new ones. Call again with
    dry_run=False to write. Only the given fields are changed.

    Args:
        site: Site code — 'esc', 'gdc', or 'cari'
        table: 'context', 'xyz', or 'datums'
        key: Identifies the record — context: {"squid": ...};
            xyz: {"squid": ..., "suffix": ...}; datums: {"name": ...}
        fields: Field values to set. Key fields can't be changed here.
        dry_run: Validate and preview without writing (default True)
    """
    state: AppState = mcp.get_context().request_context.lifespan_context
    site, table = site.lower().strip(), table.lower().strip()
    err = _validate_write(site, table)
    if err:
        return err

    key_fields = KEY_FIELDS[table]
    if set(key) != set(key_fields):
        return f"key for {table} must have exactly: {', '.join(key_fields)}"
    if not fields:
        return "No fields given to update."
    unknown = sorted(set(fields) - WRITABLE_FIELDS[table])
    if unknown:
        return (
            f"Unknown field(s) for {table}: {', '.join(unknown)}. "
            f"Writable fields: {', '.join(sorted(WRITABLE_FIELDS[table]))}"
        )
    locked = sorted(set(fields) & set(key_fields))
    if locked:
        return (
            f"Can't change key field(s) {', '.join(locked)} with update_record: the "
            "API would insert a new row instead of renaming this one."
        )

    try:
        rows = await _fetch_table(state, f"{site}/{table}/list/", refresh=True)
        if isinstance(rows, dict) and "error" in rows:
            return rows["error"]

        matches = [r for r in rows if _key_matches(r, table, key)]
        if len(matches) != 1:
            return f"Expected exactly one {table} record matching {key}, found {len(matches)}."
        current = matches[0]

        pk = current.get("squid") if table == "context" else current.get("id")
        if pk is None:
            return f"The matched {table} record has no primary key in the API response; can't update it."

        changes = {
            f: {"from": current.get(f), "to": v}
            for f, v in fields.items()
            if str(current.get(f)) != str(v)
        }
        if not changes:
            return json.dumps({"note": "No changes: the record already has these values.", "current": current},
                              indent=2, default=str)

        payload = {f: fields[f] for f in changes}
        path = f"{site}/{table}/update/{quote(str(pk), safe='')}/"
        result: dict[str, Any] = {
            "site": site, "table": table, "key": key,
            "method": "PATCH", "path": path, "changes": changes,
        }
        if table == "xyz" and {"unit", "idno"} & set(payload):
            result["warnings"] = [
                "unit/idno are also stored on the context record; changing them here "
                "can make this row disagree with its context."
            ]

        if dry_run:
            result["dry_run"] = True
            result["note"] = "Nothing written. Call again with dry_run=False to apply."
            return json.dumps(result, indent=2, default=str)

        updated = await _authed_send(state, "PATCH", path, payload)
        if isinstance(updated, dict) and "error" in updated:
            return json.dumps(updated, indent=2, default=str)
        _invalidate_site(state, site)
        result["updated"] = updated
        return json.dumps(result, indent=2, default=str)
    except httpx.HTTPStatusError as e:
        return f"HTTP error {e.response.status_code}: {e.response.text}"
    except Exception as e:
        return f"Error: {e}"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run(transport="stdio")
