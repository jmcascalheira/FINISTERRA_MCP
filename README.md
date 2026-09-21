# FINISTERRA MCP Server

MCP (Model Context Protocol) server that gives Claude direct access to the FINISTERRA project excavation database at `data.finisterra.icarehb.com`.

## Tools

| Tool | Description |
|---|---|
| `authenticate` | Connect with username/password (skip if using env vars) |
| `list_sites` | List available excavation sites |
| `get_xyz` | XYZ coordinate data for a site |
| `get_context` | Context/find data for a site |
| `get_datums` | Datum points for a site |
| `get_site_data` | Combined XYZ + Context (joined) — mirrors `finisterraR::get_site_data()` |

`get_xyz`, `get_context`, and `get_site_data` take `limit` (0 = all), `offset`, and
`count_only`.
| `get_table_schema` | Inspect field names, types, and a sample record |
| `search_by_square` | Filter XYZ records by excavation square |
| `list_squares` | Distinct square ids for a site, optionally filtered by unit |
| `field_summary` | Distinct values and counts for any field in any table |
| `summary_stats` | Quick overview: record counts, units, coordinate ranges |

Sites: **esc** (Escoural), **gdc** (Gruta da Companheira), **cari** (Carigüela)

## Pagination and caching

The upstream API has **no server-side pagination** — `limit`, `offset`, `page`, and
field filters are all ignored, and every request returns the entire table. Paging
therefore happens in this server, over a fetched table.

To stop that from re-downloading the table once per page, fetched tables are cached
in memory for `FINISTERRA_CACHE_TTL` seconds (default 300; set `0` to disable).
Walking all 8,409 ESC XYZ records in pages of 500 costs **1 HTTP fetch instead of 17**.

Paged responses carry `has_more` and `next_offset`, so you can walk a table without
tracking offsets yourself:

```
get_xyz("esc", limit=500)            -> next_offset: 500, has_more: true
get_xyz("esc", limit=500, offset=500) -> next_offset: 1000, has_more: true
```

`summary_stats` reports `n_squares` but not the ids themselves — inlining several
thousand of them used to overflow the response limit and made the tool unusable for
ESC and CARI. Use `list_squares` for the ids, which pages like any other table and
takes an optional `unit` filter:

```
list_squares("gdc", count_only=True)   -> total_records: 2060
list_squares("gdc", unit="N19")        -> 421 squares, N19-1, N19-10, ...
```

Likewise `summary_stats` lists datum *names* only; `get_datums` returns their
coordinates.

## Counting arbitrary fields

The other tools cover sites, squares and units. `field_summary` covers everything
else — `level`, `spit`, `feature`, `code`, `excavator`, `year` — without paging a
table into the conversation:

```
field_summary("esc", "context", "level")
-> distinct_values: 8, populated: 5342, blank: 18
   2=2733, 3=1756, 3b=467, 2/3=283, 4=47, 2b=28, 3/3b=15, surf=13
```

`top` caps how many distinct values come back (default 50, `0` for all), so
high-cardinality fields like `squid` truncate rather than overflow. Numeric fields
also get min/max/mean, since a list of 1,769 distinct z-coordinates is rarely useful.
An unknown field name returns the available ones.

**Placeholder nulls:** values like `NA` are real strings in the database, not nulls,
so they are counted as populated and reported separately under `null_like`. At ESC
this matters — `spit` is blank in 4,295 of 5,360 records and its *only* non-blank
value is the string `NA` (1,065 times); `feature` has 1,063 the same way. Any
`IS NOT NULL` style query over those fields will overcount badly.

**Levels are not comparable across sites:** ESC has 8 distinct values, GDC 29
(including `3/1b`, `red_entrance`, `north_profile`) and CARI 90. ESC's `2/3` and
`3/3b` are dual attributions at a contact rather than layers in their own right.

## Marker records

Some records document excavation infrastructure rather than recovered material:
those coded `PHOTO`, `TOPOGRAPHY`, `TOPO` or `POINT`. Pass `exclude_markers=True` to
`get_xyz`, `get_context`, `get_site_data`, `list_squares` or `summary_stats` to leave
them out of the counts. It defaults to off, so raw counts are unchanged.

| Site | rows (raw → filtered) | squares (raw → filtered) |
|---|---|---|
| ESC | 8,409 → 8,228 | 5,360 → 5,295 |
| GDC | 3,005 → 2,956 | 2,060 → 2,051 |
| CARI | 6,959 → 6,518 | 5,694 → 5,584 |

The filter keys on the **record code, not the unit**. That matters: units that hold
nothing but photo or survey shots (ESC `Q`, `T`, `ProfN/E/S/Nb/Es`, `N26Prof`; CARI
`trbackN/E`, `treast`, `triang`, `PdeepS/W/E`) drop out on their own, while
non-grid units that hold genuine material are kept — CARI's `corte1`/`corte2` retain
their OSL, ochre, sediment and lithic samples, and GDC's `X` keeps its stratigraphy.
Filtering by unit name instead would discard 327 real records across the three sites.

XYZ rows with no matching context record are kept: an unmatched row is a recording
gap, not evidence that the row is infrastructure.

Use `count_only=True` to size a table before pulling it — it returns just the counts,
no rows. On `get_site_data` it also reports join coverage
(`joined_with_context` / `missing_context`):

```
get_site_data("gdc", count_only=True)
-> total_records: 3005, xyz_records: 3005, context_records: 2060,
   joined_with_context: 2975, missing_context: 30
```

**Response size:** full tables far exceed the MCP response limit (ESC joined is ~4.1M
characters), so a `limit=0` pull cannot be returned inline. Page it, filter it, or have
the client write the oversized result to disk.

## Setup

### 1. Install dependencies

```bash
cd finisterra-mcp
pip install -e .
# or just:
pip install "mcp[cli]" httpx
```

### 2. Configure credentials

Set environment variables (recommended — avoids typing creds in chat):

```bash
export FINISTERRA_USERNAME="your_username"
export FINISTERRA_PASSWORD="your_password"
```

Or authenticate manually via the `authenticate` tool after connecting.

### 3. Add to Claude

#### Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "finisterra": {
      "command": "python3",
      "args": ["/absolute/path/to/finisterra-mcp/server.py"],
      "env": {
        "FINISTERRA_USERNAME": "your_username",
        "FINISTERRA_PASSWORD": "your_password"
      }
    }
  }
}
```

#### Claude Code

```bash
claude mcp add finisterra \
  -e FINISTERRA_USERNAME=your_username \
  -e FINISTERRA_PASSWORD=your_password \
  -- python3 /absolute/path/to/finisterra-mcp/server.py
```

## Usage examples

Once connected, you can ask Claude things like:

- "Show me a summary of the GDC excavation data"
- "What squares have been excavated at Escoural?"
- "Get all XYZ records from square P15 at GDC"
- "What fields are in the context table for Carigüela?"
- "Pull the combined site data for GDC and show me the depth distribution"

## Development

Run the server directly for testing:

```bash
python3 server.py
```

This starts in stdio transport mode (what Claude Desktop and Claude Code expect).
