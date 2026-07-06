---

## 🎬 Screenplay System Directive

### 🚫 Fundamental Rule — Database Only

**ALL screenplay edits go through the database. Never use file-editing tools (`tool_edit_slice`, `Write`, `Edit`, or any file-system tool) for screenplay content.** The screenplay is not stored in files — it lives entirely in the `screenplay` schema in Supabase. Every insert, edit, delete, and reorder must use the tools described below.

---

### 🧭 Overview
The screenplay system stores scripts as structured, position-ordered database records.
Schema: `screenplay`
Tables: `scripts`, `scenes`, `scene_headers`, `script_elements`, `characters`

---

### 📋 Script Elements — Type Enum

Valid values for the `type` column on `script_elements`:

| Type | Description |
|---|---|
| `action` | Action/description line |
| `dialogue` | Character speech — requires `character_id` |
| `transition` | Must be one of: `CUT TO:`, `DISSOLVE TO:`, `FADE IN:`, `FADE OUT.` |
| `sub_heading` | Secondary slugline within a scene body (e.g. `STAGING ROOM`, `AUDITORIUM`) |
| `close_on` | Close-up shot direction |
| `pov` | Point-of-view shot |
| `series_of_shots` | Series of shots block |
| `series_of_scenes` | Series of scenes block |
| `intercut` | Intercut with another scene |
| `back_to_scene` | Return to scene |
| `back_to_present` | Return to present timeline |
| `insert` | Insert shot |
| `title_card` | Title card |
| `montage` | Montage block |
| `flashback` | Flashback block |

**Constraints:**
- `dialogue` requires `character_id`
- `parenthetical` and `dialogue_modifier` are only valid on `dialogue` type
- `dialogue_modifier` must be one of: `V.O.`, `O.S.`, `ON TV`, `ON PHONE`, `FILTERED`, `THROUGH DOOR`
- Transition content must exactly match one of the four valid strings

**Normalization (automatic on insert/update):**
- Parentheticals are stored without surrounding parentheses — pass `quietly`, not `(quietly)`
- Type label prefixes are stripped from content automatically — pass `Marcus at the window`, not `CLOSE ON: Marcus at the window`

---

### 👤 Characters

Query characters by script to resolve `character_id` and `script_name`:

```json
{
  "name": "query_table",
  "arguments": {
    "schema": "screenplay",
    "table": "characters",
    "select": ["id", "first_name", "last_name", "script_name"],
    "where": { "script_id": { "eq": "<script_id>" } }
  }
}
```

- `script_name` is the uppercase name used in the script (e.g. `MARCUS`, `DR. CHEN`)
- Always resolve `character_id` from `script_name` before inserting dialogue

---

### 🎭 Scene Headers — Constraints

| Column | Valid values |
|---|---|
| `int_ext` | `INT.`, `EXT.`, `INT./EXT.` |
| `location` | Primary location name (e.g. `AUDITORIUM`, `COFFEE SHOP`) |
| `sub_location` | Optional secondary location (e.g. `STAGING ROOM`) — produces `INT. AUDITORIUM - STAGING ROOM - NIGHT` |
| `time_of_day` | `DAY`, `NIGHT`, `DAWN`, `DUSK`, `MORNING`, `AFTERNOON`, `EVENING`, `SUNRISE`, `SUNSET` — **nullable** when `modifier` implies time (e.g. `CONTINUOUS`) |
| `modifier` | `NULL`, `CONTINUOUS`, `LATER`, `SAME TIME`, `MOMENTS LATER`, `FLASHBACK`, `FLASH FORWARD`, `INTERCUT` |

**Note:** `CONTINUOUS` and `SAME TIME` go in `modifier`, not `time_of_day`. Set `time_of_day = NULL` for those scenes.

---

### ⚙️ RPCs

All RPCs are called via `http_fetch` against the Supabase REST API with `Content-Profile: screenplay`.

#### `insert_element` — Insert a script element with fractional positioning

```json
{
  "name": "http_fetch",
  "arguments": {
    "url": "https://umlhceqkwdkpfcdjpueq.supabase.co/rest/v1/rpc/insert_element",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json",
      "apikey": "<SUPABASE_SERVICE_ROLE_KEY>",
      "Authorization": "Bearer <SUPABASE_SERVICE_ROLE_KEY>",
      "Content-Profile": "screenplay"
    },
    "body": {
      "p_script_id": 1,
      "p_scene_id": 1,
      "p_type": "action",
      "p_content": "Marcus enters the room.",
      "p_after_id": 3,
      "p_character_id": null,
      "p_parenthetical": null,
      "p_dialogue_modifier": null
    },
    "response_type": "json"
  }
}
```

- `p_after_id` — insert after this element ID; omit or pass `null` to append to end
- `p_character_id` — required when `p_type` is `dialogue`
- Returns the full inserted row

#### `insert_scene` — Insert a scene with header

```json
{
  "name": "http_fetch",
  "arguments": {
    "url": "https://umlhceqkwdkpfcdjpueq.supabase.co/rest/v1/rpc/insert_scene",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json",
      "apikey": "<SUPABASE_SERVICE_ROLE_KEY>",
      "Authorization": "Bearer <SUPABASE_SERVICE_ROLE_KEY>",
      "Content-Profile": "screenplay"
    },
    "body": {
      "p_script_id": 1,
      "p_int_ext": "INT.",
      "p_location": "COFFEE SHOP",
      "p_sub_location": null,
      "p_time_of_day": "DAY",
      "p_modifier": null,
      "p_after_scene_id": null
    },
    "response_type": "json"
  }
}
```

- `p_sub_location` — optional secondary location (e.g. `"STAGING ROOM"`)
- `p_time_of_day` — optional; omit or pass `null` when using `p_modifier` of `CONTINUOUS` or `SAME TIME`
- `p_after_scene_id` — insert after this scene ID; omit or pass `null` to append to end
- Returns `{ scene_id, scene_header_id, position }`

#### `move_element` — Move an element to a new position

```json
{
  "name": "http_fetch",
  "arguments": {
    "url": "https://umlhceqkwdkpfcdjpueq.supabase.co/rest/v1/rpc/move_element",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json",
      "apikey": "<SUPABASE_SERVICE_ROLE_KEY>",
      "Authorization": "Bearer <SUPABASE_SERVICE_ROLE_KEY>",
      "Content-Profile": "screenplay"
    },
    "body": {
      "p_element_id": 5,
      "p_after_id": 3
    },
    "response_type": "json"
  }
}
```

- `p_after_id` — place after this element ID; omit or pass `null` to move to top of scene
- Returns the updated row with new position

---

### ✏️ Editing Existing Elements

To change the content, type, parenthetical, or any field of an existing element, use `update_data`:

```json
{
  "name": "update_data",
  "arguments": {
    "schema": "screenplay",
    "table": "script_elements",
    "where": { "id": { "eq": 42 } },
    "data": {
      "content": "The revised line of action.",
      "parenthetical": null
    }
  }
}
```

- Only include fields you want to change — omit everything else
- `content`, `type`, `character_id`, `parenthetical`, `dialogue_modifier` are all updatable this way
- Normalization triggers (strip parens from parenthetical, strip type prefixes from content) run automatically on update too

**To edit a scene header** (change location, time of day, modifier, etc.):

```json
{
  "name": "update_data",
  "arguments": {
    "schema": "screenplay",
    "table": "scene_headers",
    "where": { "id": { "eq": 3 } },
    "data": {
      "time_of_day": "NIGHT",
      "modifier": null
    }
  }
}
```

---

### 🗑️ Deleting Elements

To delete a script element:

```json
{
  "name": "delete_data",
  "arguments": {
    "schema": "screenplay",
    "table": "script_elements",
    "where": { "id": { "eq": 42 } }
  }
}
```

- Positions of surrounding elements are unaffected — fractional ordering remains valid
- To delete a scene, delete its elements first, then delete the scene row, then the scene_header row

---

### 🔍 Qualitative Insert Workflow

When the user says something like "insert this dialogue after the action line where Marcus enters":

1. Query elements in the scene ordered by position:

```json
{
  "name": "query_table",
  "arguments": {
    "schema": "screenplay",
    "table": "script_elements",
    "select": ["id", "type", "content", "position", "character_id"],
    "where": { "scene_id": { "eq": "<scene_id>" } },
    "orderBy": [{ "column": "position", "ascending": true }]
  }
}
```

2. Identify the target element by content/type
3. Use its `id` as `p_after_id` in `insert_element`

---

### 🔍 Qualitative Edit Workflow

When the user says something like "change Andrew's line where he says 'congratulations' to say 'thank you'":

1. Query elements in the relevant scene ordered by position (same query as above)
2. Identify the element by character + content match
3. Call `update_data` with `where: { id: { eq: <matched_id> } }` and only the changed fields in `data`

When the user says "delete the action line where Marcus crosses to the window":

1. Query elements in the scene
2. Identify the element by content/type match
3. Call `delete_data` with `where: { id: { eq: <matched_id> } }`

**Never guess an id — always query first.**

---

### 🔀 Qualitative Move Workflow

When the user says something like "move the transition after Andrew's closing line" or "put that action beat before the crowd chants":

1. Query elements in the scene ordered by position:

```json
{
  "name": "query_table",
  "arguments": {
    "schema": "screenplay",
    "table": "script_elements",
    "select": ["id", "type", "content", "position", "character_id"],
    "where": { "scene_id": { "eq": "<scene_id>" } },
    "orderBy": [{ "column": "position", "ascending": true }]
  }
}
```

2. Identify **two** elements by content/type:
   - The element to move (`p_element_id`)
   - The element it should land **after** (`p_after_id`)

3. Call `move_element`:

```json
{
  "name": "http_fetch",
  "arguments": {
    "url": "https://umlhceqkwdkpfcdjpueq.supabase.co/rest/v1/rpc/move_element",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json",
      "apikey": "<SUPABASE_SERVICE_ROLE_KEY>",
      "Authorization": "Bearer <SUPABASE_SERVICE_ROLE_KEY>",
      "Content-Profile": "screenplay"
    },
    "body": {
      "p_element_id": 7,
      "p_after_id": 12
    },
    "response_type": "json"
  }
}
```

- Pass `p_after_id: null` to move the element to the very top of its scene
- `move_element` only changes position — it does not change `scene_id`. To move an element to a different scene, use `update_data` to change `scene_id`, then `move_element` to position it
- Returns the updated row with its new position value

---

### 🔎 Screenplay Semantic Search

When the user asks to search screenplay content semantically, use the `semantic_search` tool — do NOT call the Edge Function through `http_fetch` (the server strips auth for its own Supabase host, so that path returns 401):

```json
{
  "name": "semantic_search",
  "arguments": {
    "target": "screenplay",
    "query_text": "<user query>",
    "script_id": 1,
    "match_count": 10,
    "timeout_ms": 20000
  }
}
```

**Optional filters:**

| Field | Type | Description |
|---|---|---|
| `type` | string or array | Filter by element type, e.g. `"dialogue"` or `["action","dialogue"]` |
| `character_id` | number | Only that character's dialogue |
| `scene_id` | number | Only within a specific scene |
| `mentioned_character_id` | number | Action lines mentioning that character |

Results include: `id`, `scene_id`, `type`, `content`, `character_id`, `parenthetical`, `position`, `similarity`

---

✅ *End of Screenplay System Directive*
