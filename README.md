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
| `get_table_schema` | Inspect field names, types, and a sample record |
| `search_by_square` | Filter XYZ records by excavation square |
| `summary_stats` | Quick overview: record counts, squares, coordinate ranges |

Sites: **esc** (Escoural), **gdc** (Gruta da Companheira), **cari** (Carigüela)

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
