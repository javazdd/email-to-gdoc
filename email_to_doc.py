#!/usr/bin/env python3
"""
Email to Google Doc aggregator — with inline image support.

Searches Gmail for [Feature Announcement][GA] emails in a given month,
extracts the external release note section (with images), and creates
a formatted Google Doc with ToC, page breaks, and hyperlinks.
"""

import os, base64, re, calendar, html, struct, io, time, urllib.request, ssl
import xml.etree.ElementTree as ET
from datetime import date as _date
from email.utils import parsedate as _parsedate
from html.parser import HTMLParser
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.cloud import storage as gcs_lib

SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/devstorage.full_control',
]

GCS_PROJECT = 'datadog-sandbox'
GCS_BUCKET  = 'javier-feature-announcement-images'  # must be globally unique

CREDENTIALS_PATH = os.path.expanduser('~/Documents/Claude/credentials.json')
TOKEN_PATH       = os.path.expanduser('~/Documents/Claude/token.json')


def utf16_len(s):
    """Google Docs API indices are UTF-16 code unit counts, not Python char counts."""
    return len(s.encode('utf-16-le')) // 2


def docs_batch_update(docs_service, doc_id, requests, retries=6):
    """Execute a batchUpdate with exponential backoff on rate-limit errors."""
    delay = 10
    for attempt in range(retries):
        try:
            return docs_service.documents().batchUpdate(
                documentId=doc_id, body={'requests': requests}
            ).execute()
        except Exception as e:
            if '429' in str(e) and attempt < retries - 1:
                print(f'  [rate limit] waiting {delay}s ...')
                time.sleep(delay)
                delay = min(delay * 2, 120)
            else:
                raise


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_credentials():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'w') as f:
            f.write(creds.to_json())
    return creds


# ---------------------------------------------------------------------------
# Email parsing — HTML with inline image placeholder support
# ---------------------------------------------------------------------------

class _HTMLParserWithImages(HTMLParser):
    """
    Converts HTML to plain text.
    - Block tags produce newlines
    - <img src="cid:..."> becomes IMG_PLACEHOLDER_N
    - <a href="url">text</a> becomes LNKS{n}text LNKE{n} so we can restore hyperlinks
    """

    BLOCK_TAGS = {'p', 'div', 'br', 'tr', 'td', 'li',
                  'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                  'blockquote', 'pre', 'hr', 'table', 'thead', 'tbody', 'section'}

    def __init__(self):
        super().__init__()
        self.chunks      = []
        self._img_n      = 0
        self.img_map     = {}   # placeholder -> cid
        self._link_n     = 0
        self.link_map    = {}   # N -> url
        self._link_stack = []   # nested <a> tracking

    def handle_starttag(self, tag, attrs):
        if tag in self.BLOCK_TAGS:
            self.chunks.append('\n')
        if tag == 'img':
            d   = dict(attrs)
            src = d.get('src', '')
            if src.lower().startswith('cid:'):
                cid = src[4:].strip()
                ph  = f'IMG_PLACEHOLDER_{self._img_n}'
                self._img_n += 1
                self.img_map[ph] = cid
                self.chunks.append(f'\n{ph}\n')
        if tag == 'a':
            d    = dict(attrs)
            href = d.get('href', '')
            if href.startswith('http'):
                n = self._link_n
                self._link_n += 1
                self.link_map[n] = href
                self._link_stack.append(n)
                self.chunks.append(f'LNKS{n}Z')  # Z = field delimiter

    def handle_endtag(self, tag):
        if tag in self.BLOCK_TAGS:
            self.chunks.append('\n')
        if tag == 'a' and self._link_stack:
            n = self._link_stack.pop()
            self.chunks.append(f'LNKE{n}Z')

    def handle_data(self, data):
        self.chunks.append(data)

    def get_text(self):
        return ''.join(self.chunks)


def get_email_body_and_images(msg):
    """
    Returns (text, img_map, inline_atts):
      - text:        plain text with IMG_PLACEHOLDER_N markers
      - img_map:     {placeholder_name: cid}
      - inline_atts: {cid: {attachment_id, inline_data, mime_type, filename}}
    """
    payload = msg.get('payload', {})

    # --- collect inline image attachments keyed by content-id ---
    inline_atts = {}

    def find_atts(part):
        mime = part.get('mimeType', '')
        if mime.startswith('image/'):
            hdrs = {h['name'].lower(): h['value'] for h in part.get('headers', [])}
            cid  = hdrs.get('content-id', '').strip('<>')
            fn   = part.get('filename', '') or f'image.{mime.split("/")[-1]}'
            if cid:
                inline_atts[cid] = {
                    'mime_type':     mime,
                    'filename':      fn,
                    'attachment_id': part.get('body', {}).get('attachmentId'),
                    'inline_data':   part.get('body', {}).get('data'),
                }
        for sub in part.get('parts', []):
            find_atts(sub)

    find_atts(payload)

    def decode(part):
        d = part.get('body', {}).get('data', '')
        return base64.urlsafe_b64decode(d).decode('utf-8', errors='replace') if d else ''

    def find_part(p, mime):
        if p.get('mimeType') == mime:
            return decode(p)
        for sub in p.get('parts', []):
            r = find_part(sub, mime)
            if r:
                return r
        return ''

    # Prefer HTML so we can extract image positions and hyperlinks
    html_body = find_part(payload, 'text/html')
    if html_body.strip():
        parser = _HTMLParserWithImages()
        parser.feed(html.unescape(html_body))
        return parser.get_text(), parser.img_map, parser.link_map, inline_atts

    # Fall back to plain text (no images/links extractable)
    return find_part(payload, 'text/plain'), {}, {}, inline_atts


# ---------------------------------------------------------------------------
# Content helpers
# ---------------------------------------------------------------------------

def extract_release_note(body):
    pattern = re.compile(
        r'[-—–]+\s*Release Note can be shared externally\s*[-—–]+',
        re.IGNORECASE
    )
    m = pattern.search(body)
    return body[m.end():].strip() if m else None


def parse_subject(subject):
    """
    '[Feature Announcement][Incident App][Chat Integration][GA] Manage incidents...'
    -> ('Incident App - Chat Integration - Manage incidents...', 'Manage incidents...')
    """
    cleaned  = re.sub(r'\[Feature Announcement\]', '', subject, flags=re.IGNORECASE)
    cleaned  = re.sub(r'\[GA\]',      '', cleaned, flags=re.IGNORECASE)
    cleaned  = re.sub(r'\[Preview\]', '', cleaned, flags=re.IGNORECASE)
    brackets = re.findall(r'\[([^\]]+)\]', cleaned)
    plain    = re.sub(r'\[[^\]]+\]', '', cleaned).strip()
    parts    = brackets + ([plain] if plain else [])
    return ' - '.join(parts), (plain if plain else ' - '.join(brackets))


def clean_content(content, feature_heading):
    """
    Strip all boilerplate from release note content.
    IMG_PLACEHOLDER_N markers are preserved for later image insertion.
    """
    # Tags metadata — handle ASCII quotes, Unicode curly quotes, or no quotes
    content = re.sub(r'[\u201c\u201d"]?tags[\u201c\u201d"]?\s*:\s*\[[^\]]*\]', '',
                     content, flags=re.DOTALL | re.IGNORECASE)

    # [image: ...] plain-text email artifacts
    content = re.sub(r'\[image:[^\]]*\]', '', content, flags=re.IGNORECASE)

    # Angle-bracketed URLs → bare URL
    content = re.sub(r'<(https?://[^\s>]+)>', r'\1', content)

    # *bold* markdown markers
    content = re.sub(r'\*([^*\n]+)\*', r'\1', content)

    # Normalize line endings
    content = content.replace('\r\n', '\n').replace('\r', '\n')

    # End-of-shareable-content dividers — various phrasings, strip everything after
    content = re.sub(
        r'\s*[-—–]+\s*(end external copy|end of public.facing message)\s*[-—–]*.*',
        '', content, flags=re.IGNORECASE | re.DOTALL
    )

    # Email sig separator "-- " and everything after (Google Groups footer, sigs)
    content = re.sub(r'\n--[ \t]*\n.*', '', content, flags=re.DOTALL)
    content = re.sub(r'\n[-—–]{2}[ \t]*\n.*', '', content, flags=re.DOTALL)

    lines = content.split('\n')

    # Remove first non-empty line if it duplicates the feature heading
    first_idx = next((i for i, l in enumerate(lines) if l.strip()), None)
    if first_idx is not None:
        norm = lambda s: re.sub(r'\s+', ' ', s.lower().strip())
        if norm(lines[first_idx]) in norm(feature_heading) or \
           norm(feature_heading) in norm(lines[first_idx]):
            lines = lines[:first_idx] + lines[first_idx + 1:]

    # Signature openers — also catch "Best,Name" run together (no newline from HTML)
    sig_re = re.compile(
        r'^(best|regards|thanks|cheers|sincerely|warm regards|thank you)[,.]?',
        re.IGNORECASE
    )
    for i, line in enumerate(lines):
        if sig_re.match(line.strip()):
            lines = lines[:i]
            break

    result = re.sub(r'\n{3,}', '\n\n', '\n'.join(lines))
    result = result.strip()

    # Strip trailing signature blocks ending with "Datadog"
    # Handles both multi-line ("...\nDatadog") and collapsed ("...ManagerDatadog")
    lines_list = result.split('\n')
    i = len(lines_list) - 1
    while i >= 0 and not lines_list[i].strip():
        i -= 1
    if i >= 0:
        last_line = lines_list[i].strip()
        if last_line == 'Datadog' or last_line.endswith('Datadog'):
            cut_at = i
            # Walk backwards: strip additional short, punctuation-free sig lines
            j = i - 1
            while j >= 0:
                above = lines_list[j].strip()
                if not above:
                    j -= 1
                    continue
                # Real content: long line or ends with terminal punctuation
                if len(above) > 80 or above[-1] in '.!?:':
                    break
                cut_at = j
                j -= 1
            result = '\n'.join(lines_list[:cut_at]).strip()

    return result


# ---------------------------------------------------------------------------
# Image helpers
# ---------------------------------------------------------------------------

def download_image(gmail_service, msg_id, att_info):
    """Download image bytes from a Gmail attachment or inline body data."""
    att_id = att_info.get('attachment_id')
    if att_id:
        att = gmail_service.users().messages().attachments().get(
            userId='me', messageId=msg_id, id=att_id
        ).execute()
        return base64.urlsafe_b64decode(att.get('data', ''))
    idata = att_info.get('inline_data')
    if idata:
        return base64.urlsafe_b64decode(idata)
    return None


def ensure_gcs_bucket(storage_client):
    """Return the GCS bucket, creating it if it doesn't exist."""
    try:
        return storage_client.get_bucket(GCS_BUCKET)
    except Exception:
        bucket = storage_client.create_bucket(GCS_BUCKET, location='US')
        return bucket


def upload_image_to_gcs(storage_client, img_bytes, filename, mime_type):
    """
    Upload image to GCS and make it publicly readable.
    Returns the public URL.
    """
    bucket = ensure_gcs_bucket(storage_client)
    # Avoid filename collisions by prefixing with a short hash
    safe_name = re.sub(r'[^\w.\-]', '_', filename)
    blob_name = f'{abs(hash(img_bytes[:64]))}_{safe_name}'
    blob = bucket.blob(blob_name)
    blob.upload_from_string(img_bytes, content_type=mime_type,
                            predefined_acl='publicRead')
    return blob.public_url


def image_size_pt(img_bytes, mime_type, max_w_pt=468.0):
    """Return (width_pt, height_pt) scaled to fit max_w_pt (6.5" page width)."""
    w = h = None
    try:
        if 'png' in mime_type:
            w = struct.unpack('>I', img_bytes[16:20])[0]
            h = struct.unpack('>I', img_bytes[20:24])[0]
        elif 'jpeg' in mime_type or 'jpg' in mime_type:
            i = 2
            while i + 9 < len(img_bytes):
                if img_bytes[i] != 0xFF:
                    break
                mk = img_bytes[i + 1]
                if mk in (0xC0, 0xC1, 0xC2):
                    h = struct.unpack('>H', img_bytes[i+5:i+7])[0]
                    w = struct.unpack('>H', img_bytes[i+7:i+9])[0]
                    break
                sz = struct.unpack('>H', img_bytes[i+2:i+4])[0]
                i += 2 + sz
    except Exception:
        pass
    if w and h and w > 0:
        w_pt = w * 72.0 / 96.0   # 96 DPI screen resolution assumption
        h_pt = h * 72.0 / 96.0
        if w_pt > max_w_pt:
            h_pt = h_pt * max_w_pt / w_pt
            w_pt = max_w_pt
        return w_pt, h_pt
    return 400.0, 250.0  # fallback


def find_placeholders_in_doc(doc_content):
    """Scan document body text runs and return {placeholder: (start, end)}."""
    result = {}
    for elem in doc_content.get('body', {}).get('content', []):
        para = elem.get('paragraph', {})
        for run in para.get('elements', []):
            tr = run.get('textRun', {})
            if not tr:
                continue
            text  = tr.get('content', '')
            start = run.get('startIndex', 0)
            for m in re.finditer(r'IMG_PLACEHOLDER_\d+', text):
                ph = m.group(0)
                result[ph] = (start + m.start(), start + m.end())
    return result


def find_link_markers_in_doc(doc_content):
    """
    Scan document body and return positions of LNKS/LNKE markers.
    Returns: {n: {'start_marker': (s, e), 'end_marker': (s, e)}}
    using UTF-16 code unit offsets.
    """
    markers = []
    for elem in doc_content.get('body', {}).get('content', []):
        para = elem.get('paragraph', {})
        for run in para.get('elements', []):
            tr = run.get('textRun', {})
            if not tr:
                continue
            text      = tr.get('content', '')
            run_start = run.get('startIndex', 0)
            for m in re.finditer(r'LNKS(\d+)Z|LNKE(\d+)Z', text):
                char_offset = utf16_len(text[:m.start()])
                abs_start   = run_start + char_offset
                abs_end     = abs_start + utf16_len(m.group(0))
                if m.group(1) is not None:
                    markers.append(('S', int(m.group(1)), abs_start, abs_end))
                else:
                    markers.append(('E', int(m.group(2)), abs_start, abs_end))
    result = {}
    for kind, n, s, e in markers:
        result.setdefault(n, {})
        result[n]['start_marker' if kind == 'S' else 'end_marker'] = (s, e)
    return result


# ---------------------------------------------------------------------------
# Google Doc creation
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Events / Summits scraper
# ---------------------------------------------------------------------------

SUMMITS_URL = 'https://events.datadoghq.com/summits'
EVENTS_BASE  = 'https://events.datadoghq.com'


class _SummitsPageParser(HTMLParser):
    """
    Parses events.datadoghq.com/summits.

    The page uses data-title, data-date, and class on the card element itself:
      <a href="/summits/..." class="summit-card group is-upcoming"
         data-title="Datadog Summit Bengaluru" data-date="April 16, 2026">
      <a href="/summits/..." class="summit-card group is-past"  ...>
      <div class="summit-card is-coming-soon" data-title="Datadog Summit Chicago">

    All data needed is in the tag attributes — no content parsing required.
    """

    def __init__(self):
        super().__init__()
        self.items = []   # (name, label, url, is_upcoming)

    def handle_starttag(self, tag, attrs):
        if tag not in ('a', 'div'):
            return
        d   = dict(attrs)
        cls = d.get('class', '')
        if 'summit-card' not in cls:
            return

        name = d.get('data-title', '').strip()
        if not name:
            return

        label = d.get('data-date', '').strip() or 'Coming Soon'

        href  = d.get('href', '')
        url   = None
        if href:
            url = href if href.startswith('http') else EVENTS_BASE + href

        is_upcoming = 'is-past' not in cls   # upcoming OR coming-soon

        self.items.append((name, label, url, is_upcoming))


def fetch_summits():
    """
    Fetches events.datadoghq.com/summits and returns:
      upcoming  — (name, label, url) for events today or in the future
      on_demand — (name, label, url) for past events (already happened)
    """
    try:
        req = urllib.request.Request(
            SUMMITS_URL,
            headers={'User-Agent': 'Mozilla/5.0 (compatible; email_to_doc/1.0)'},
        )
        ctx                = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            raw = resp.read().decode('utf-8', errors='replace')
    except Exception as e:
        print(f'Warning: could not fetch summits page ({e})')
        return [], []

    p = _SummitsPageParser()
    p.feed(html.unescape(raw))
    upcoming  = [(n, l, u) for n, l, u, up in p.items if up]
    on_demand = [(n, l, u) for n, l, u, up in p.items if not up]
    return upcoming, on_demand


BLOG_RSS_URL     = 'https://www.datadoghq.com/blog/index.xml'
ENABLEMENT_URL   = 'https://www.datadoghq.com/technical-enablement/sessions/'
ENABLEMENT_BASE  = 'https://www.datadoghq.com'


def fetch_blog_posts(months_back=3):
    """
    Fetch the Datadog blog RSS feed and return posts published in the last
    `months_back` calendar months from today.

    Returns a list of (title, url, pub_date) sorted newest-first,
    grouped as an OrderedDict: {month_label: [(title, url, date_str), ...]}
    """
    from collections import OrderedDict

    today        = _date.today()
    cutoff_month = today.month - months_back
    cutoff_year  = today.year
    while cutoff_month <= 0:
        cutoff_month += 12
        cutoff_year  -= 1
    cutoff = _date(cutoff_year, cutoff_month, 1)

    try:
        req = urllib.request.Request(
            BLOG_RSS_URL,
            headers={'User-Agent': 'Mozilla/5.0 (compatible; email_to_doc/1.0)'},
        )
        ctx                = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            raw = resp.read()
    except Exception as e:
        print(f'Warning: could not fetch blog RSS ({e})')
        return OrderedDict()

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        print(f'Warning: could not parse blog RSS ({e})')
        return OrderedDict()

    posts = []
    for item in root.findall('./channel/item'):
        title   = (item.findtext('title') or '').strip()
        link    = (item.findtext('link')  or '').strip()
        pub_raw = (item.findtext('pubDate') or '').strip()
        if not (title and link and pub_raw):
            continue
        t = _parsedate(pub_raw)
        if not t:
            continue
        pub = _date(t[0], t[1], t[2])
        if pub < cutoff:
            break   # RSS is newest-first; once we're past the window, stop
        date_str = f'{pub.strftime("%B")} {pub.day}, {pub.year}'
        posts.append((title, link, date_str, pub))

    # Group by "Month Year" label, preserving newest-first order within each group
    grouped = OrderedDict()
    for title, link, date_str, pub in posts:
        month_label = pub.strftime('%B %Y')
        grouped.setdefault(month_label, []).append((title, link, date_str))

    print(f'  {sum(len(v) for v in grouped.values())} blog post(s) across {len(grouped)} month(s)')
    return grouped


def fetch_training_sessions():
    """
    Fetch the Datadog Technical Enablement sessions page and return upcoming
    English-language sessions sorted by next scheduled date.

    Returns a list of (title, url, next_date_str, topics) where:
      - title         session title
      - url           full URL to the session page
      - next_date_str e.g. "Apr 07, 2026"
      - topics        list of topic strings (e.g. ["APM", "Logs"])
    """
    from datetime import datetime, timezone

    try:
        req = urllib.request.Request(
            ENABLEMENT_URL,
            headers={'User-Agent': 'Mozilla/5.0 (compatible; email_to_doc/1.0)'},
        )
        ctx                = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            raw = resp.read().decode('utf-8', errors='replace')
    except Exception as e:
        print(f'Warning: could not fetch enablement page ({e})')
        return []

    # Session data is embedded as an inline JS variable in the HTML
    import json as _json
    m = re.search(r'const allSessions = (\[.*?\]);\s*\(function', raw, re.DOTALL)
    if not m:
        m = re.search(r'const allSessions = (\[.*?\]);', raw, re.DOTALL)
    if not m:
        print('Warning: could not locate allSessions data in enablement page')
        return []

    try:
        data = _json.loads(m.group(1))
    except Exception as e:
        print(f'Warning: could not parse allSessions JSON ({e})')
        return []

    now = datetime.now(timezone.utc)
    results = []

    for session in data:
        # Only English sessions
        lang = (session.get('tem_language') or '').lower()
        if lang and lang != 'english':
            continue

        title = (session.get('header_title') or '').strip()
        if not title:
            continue

        path = session.get('relpermalink') or session.get('id') or ''
        url  = ENABLEMENT_BASE + path if path else ''

        # Find future available slots
        future_slots = [
            s for s in session.get('sessions', [])
            if s.get('status') == 'available'
            and datetime.fromisoformat(s['start_time'].replace('Z', '+00:00')) > now
        ]
        if not future_slots:
            continue

        next_dt = min(
            datetime.fromisoformat(s['start_time'].replace('Z', '+00:00'))
            for s in future_slots
        )
        next_date_str = next_dt.strftime('%b %d, %Y')

        # Normalize topic names (e.g. "apm" → "APM", "slos" → "SLOs")
        _abbrevs = {'apm': 'APM', 'slos': 'SLOs', 'cnm': 'CNM', 'rum': 'RUM',
                    'llm observability': 'LLM Observability', 'ci': 'CI'}
        raw_topics = session.get('tem_topics') or []
        topics = []
        for t in raw_topics:
            tl = t.lower()
            topics.append(_abbrevs.get(tl, t.capitalize()))

        results.append((next_dt, title, url, next_date_str, topics))

    results.sort(key=lambda x: x[0])
    print(f'  {len(results)} upcoming English training session(s)')
    return [(title, url, date_str, topics) for _, title, url, date_str, topics in results]


def create_google_doc(docs_service, gmail_service, storage_client, title, features,
                      upcoming_events=(), on_demand_summits=(),
                      blog_posts=None, training_sessions=None):
    """
    features:           list of (toc_entry, heading, content, img_map, link_map, inline_atts, msg_id, section)
    upcoming_events:    list of (name, label, url)  — added as "Events" section
    on_demand_summits:  list of (name, label, url)  — added as "Summits" section
    blog_posts:         OrderedDict {month_label: [(title, url, date_str), ...]}
    training_sessions:  list of (title, url, next_date_str, topics)
    """
    if blog_posts is None:
        blog_posts = {}
    if training_sessions is None:
        training_sessions = []
    doc    = docs_service.documents().create(body={'title': title}).execute()
    doc_id = doc['documentId']

    # Group features by section, preserving insertion order
    from collections import OrderedDict
    sections = OrderedDict()
    for i, feat in enumerate(features):
        sname = feat[7]
        sections.setdefault(sname, []).append(i)   # list of global feature indices

    # ------------------------------------------------------------------
    # Pass 1 — insert all content text + paragraph/text styles
    # ------------------------------------------------------------------
    pos      = 1
    segments = []

    def add(text, style=None, seg_type=None, feature_idx=None, **extra):
        nonlocal pos
        if not text:
            return
        text = text.replace('\r\n', '\n').replace('\r', '\n')
        tl   = utf16_len(text)
        seg  = {
            'text':        text,
            'style':       style,
            'type':        seg_type,
            'feature_idx': feature_idx,
            'start':       pos,
            'end':         pos + tl,
        }
        seg.update(extra)
        segments.append(seg)
        pos += tl

    # ---- ToC ----
    add('Table of Contents\n', style='HEADING_1', seg_type='toc_header')
    add('\n')
    for sname, idxs in sections.items():
        add(f'{sname}\n', seg_type='toc_section_label')
        for i in idxs:
            add(f'• {features[i][0]}\n', seg_type='toc_entry', feature_idx=i)
        add('\n')
    add('\n')

    # ---- Content sections (GA / Preview features) ----
    for sname, idxs in sections.items():
        add(f'{sname}\n', style='HEADING_1', seg_type='section_header')
        for i in idxs:
            add(f'{features[i][1]}\n', style='HEADING_2', seg_type='feature_heading', feature_idx=i)
            add(f'{features[i][2]}\n\n', seg_type='content', feature_idx=i)

    # ---- Events section (upcoming summits) ----
    if upcoming_events:
        add('Events\n', style='HEADING_1', seg_type='section_header')
        for name, label, url in upcoming_events:
            line = f'• {name} — {label}\n'
            add(line, seg_type='event_item', event_name=name, event_url=url)

    # ---- Summits section (on-demand) ----
    if on_demand_summits:
        add('Summits\n', style='HEADING_1', seg_type='section_header')
        for name, label, url in on_demand_summits:
            line = f'• {name}\n'
            add(line, seg_type='event_item', event_name=name, event_url=url)

    # ---- Blog Posts section ----
    if blog_posts:
        add('Blog Posts\n', style='HEADING_1', seg_type='section_header')
        for month_label, posts in blog_posts.items():
            add(f'{month_label}\n', seg_type='blog_month_header')
            for post_title, post_url, date_str in posts:
                add(f'• {post_title}\n',
                    seg_type='blog_post', post_title=post_title, post_url=post_url)

    # ---- Training / Enablement section ----
    if training_sessions:
        add('Training / Enablement\n', style='HEADING_1', seg_type='section_header')
        for t_title, t_url, t_date, t_topics in training_sessions:
            topic_str = f' [{", ".join(t_topics)}]' if t_topics else ''
            line = f'• {t_title} — {t_date}{topic_str}\n'
            add(line, seg_type='training_item',
                training_title=t_title, training_url=t_url)

    full_text = ''.join(s['text'] for s in segments)

    requests = [
        {'insertText': {'location': {'index': 1}, 'text': full_text}}
    ]

    # Named heading styles
    for seg in segments:
        if seg['style']:
            requests.append({
                'updateParagraphStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'paragraphStyle': {'namedStyleType': seg['style']},
                    'fields': 'namedStyleType',
                }
            })

    # ---- Section headers (GA / Preview): centered, Arial 20pt bold ----
    for seg in segments:
        if seg['type'] == 'section_header':
            requests.append({
                'updateParagraphStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'paragraphStyle': {
                        'alignment':  'CENTER',
                        'spaceAbove': {'magnitude': 0,  'unit': 'PT'},
                        'spaceBelow': {'magnitude': 0,  'unit': 'PT'},
                    },
                    'fields': 'alignment,spaceAbove,spaceBelow',
                }
            })
            requests.append({
                'updateTextStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'textStyle': {
                        'fontSize':           {'magnitude': 22, 'unit': 'PT'},
                        'weightedFontFamily': {'fontFamily': 'Arial'},
                        'bold':               True,
                    },
                    'fields': 'fontSize,weightedFontFamily,bold',
                }
            })

    # ---- ToC section labels: Arial 12pt bold ----
    for seg in segments:
        if seg['type'] == 'toc_section_label':
            requests.append({
                'updateParagraphStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'paragraphStyle': {
                        'spaceAbove': {'magnitude': 8, 'unit': 'PT'},
                        'spaceBelow': {'magnitude': 4, 'unit': 'PT'},
                    },
                    'fields': 'spaceAbove,spaceBelow',
                }
            })
            requests.append({
                'updateTextStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'textStyle': {
                        'fontSize':           {'magnitude': 12, 'unit': 'PT'},
                        'weightedFontFamily': {'fontFamily': 'Arial'},
                        'bold':               True,
                    },
                    'fields': 'fontSize,weightedFontFamily,bold',
                }
            })

    # ---- Body text: Arial 11pt, 1.15 line spacing, 8pt paragraph spacing ----
    for seg in segments:
        if seg['type'] in ('content', 'toc_entry'):
            requests.append({
                'updateParagraphStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'paragraphStyle': {
                        'lineSpacing':  115,
                        'spaceBelow':   {'magnitude': 8,  'unit': 'PT'},
                        'spaceAbove':   {'magnitude': 0,  'unit': 'PT'},
                    },
                    'fields': 'lineSpacing,spaceBelow,spaceAbove',
                }
            })
            requests.append({
                'updateTextStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'textStyle': {
                        'fontSize':           {'magnitude': 11, 'unit': 'PT'},
                        'weightedFontFamily': {'fontFamily': 'Arial'},
                    },
                    'fields': 'fontSize,weightedFontFamily',
                }
            })

    # ---- Feature headings: Arial 16 bold, generous spacing above/below ----
    for seg in segments:
        if seg['type'] == 'feature_heading':
            requests.append({
                'updateParagraphStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'paragraphStyle': {
                        'spaceAbove': {'magnitude': 0,  'unit': 'PT'},
                        'spaceBelow': {'magnitude': 12, 'unit': 'PT'},
                    },
                    'fields': 'spaceAbove,spaceBelow',
                }
            })
            requests.append({
                'updateTextStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'textStyle': {
                        'fontSize':           {'magnitude': 16, 'unit': 'PT'},
                        'weightedFontFamily': {'fontFamily': 'Arial'},
                        'bold':               True,
                    },
                    'fields': 'fontSize,weightedFontFamily,bold',
                }
            })

    # URL hyperlinks in content
    for seg in segments:
        if seg['type'] == 'content':
            for m in re.finditer(r'https?://\S+', seg['text']):
                url     = m.group(0).rstrip('.,;)')
                u_start = seg['start'] + utf16_len(seg['text'][:m.start()])
                u_end   = min(u_start + utf16_len(url), seg['end'])
                if u_start < u_end:
                    requests.append({
                        'updateTextStyle': {
                            'range': {'startIndex': u_start, 'endIndex': u_end},
                            'textStyle': {'link': {'url': url}},
                            'fields': 'link',
                        }
                    })

    # ---- Event / Summit items: Arial 11pt body style + hyperlink on name ----
    for seg in segments:
        if seg['type'] == 'event_item':
            requests.append({
                'updateParagraphStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'paragraphStyle': {
                        'lineSpacing': 115,
                        'spaceBelow':  {'magnitude': 6,  'unit': 'PT'},
                        'spaceAbove':  {'magnitude': 0,  'unit': 'PT'},
                    },
                    'fields': 'lineSpacing,spaceBelow,spaceAbove',
                }
            })
            requests.append({
                'updateTextStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'textStyle': {
                        'fontSize':           {'magnitude': 11, 'unit': 'PT'},
                        'weightedFontFamily': {'fontFamily': 'Arial'},
                    },
                    'fields': 'fontSize,weightedFontFamily',
                }
            })
            # Hyperlink just the event name (after "• ")
            url = seg.get('event_url')
            if url:
                name    = seg['event_name']
                lnk_s   = seg['start'] + utf16_len('• ')
                lnk_e   = lnk_s + utf16_len(name)
                requests.append({
                    'updateTextStyle': {
                        'range': {'startIndex': lnk_s, 'endIndex': lnk_e},
                        'textStyle': {'link': {'url': url}},
                        'fields': 'link',
                    }
                })

    # ---- Blog month headers: Arial 12pt bold ----
    for seg in segments:
        if seg['type'] == 'blog_month_header':
            requests.append({
                'updateParagraphStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'paragraphStyle': {
                        'spaceAbove': {'magnitude': 14, 'unit': 'PT'},
                        'spaceBelow': {'magnitude': 4,  'unit': 'PT'},
                    },
                    'fields': 'spaceAbove,spaceBelow',
                }
            })
            requests.append({
                'updateTextStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'textStyle': {
                        'fontSize':           {'magnitude': 12, 'unit': 'PT'},
                        'weightedFontFamily': {'fontFamily': 'Arial'},
                        'bold':               True,
                    },
                    'fields': 'fontSize,weightedFontFamily,bold',
                }
            })

    # ---- Blog post items: Arial 11pt, title hyperlinked ----
    for seg in segments:
        if seg['type'] == 'blog_post':
            requests.append({
                'updateParagraphStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'paragraphStyle': {
                        'lineSpacing': 115,
                        'spaceBelow':  {'magnitude': 4, 'unit': 'PT'},
                        'spaceAbove':  {'magnitude': 0, 'unit': 'PT'},
                    },
                    'fields': 'lineSpacing,spaceBelow,spaceAbove',
                }
            })
            requests.append({
                'updateTextStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'textStyle': {
                        'fontSize':           {'magnitude': 11, 'unit': 'PT'},
                        'weightedFontFamily': {'fontFamily': 'Arial'},
                    },
                    'fields': 'fontSize,weightedFontFamily',
                }
            })
            post_url = seg.get('post_url')
            if post_url:
                title = seg['post_title']
                lnk_s = seg['start'] + utf16_len('• ')
                lnk_e = lnk_s + utf16_len(title)
                requests.append({
                    'updateTextStyle': {
                        'range': {'startIndex': lnk_s, 'endIndex': lnk_e},
                        'textStyle': {'link': {'url': post_url}},
                        'fields': 'link',
                    }
                })

    # ---- Training / Enablement items: Arial 11pt, title hyperlinked ----
    for seg in segments:
        if seg['type'] == 'training_item':
            requests.append({
                'updateParagraphStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'paragraphStyle': {
                        'lineSpacing': 115,
                        'spaceBelow':  {'magnitude': 6, 'unit': 'PT'},
                        'spaceAbove':  {'magnitude': 0, 'unit': 'PT'},
                    },
                    'fields': 'lineSpacing,spaceBelow,spaceAbove',
                }
            })
            requests.append({
                'updateTextStyle': {
                    'range': {'startIndex': seg['start'], 'endIndex': seg['end']},
                    'textStyle': {
                        'fontSize':           {'magnitude': 11, 'unit': 'PT'},
                        'weightedFontFamily': {'fontFamily': 'Arial'},
                    },
                    'fields': 'fontSize,weightedFontFamily',
                }
            })
            t_url = seg.get('training_url')
            if t_url:
                t_title = seg['training_title']
                lnk_s   = seg['start'] + utf16_len('• ')
                lnk_e   = lnk_s + utf16_len(t_title)
                requests.append({
                    'updateTextStyle': {
                        'range': {'startIndex': lnk_s, 'endIndex': lnk_e},
                        'textStyle': {'link': {'url': t_url}},
                        'fields': 'link',
                    }
                })

    docs_batch_update(docs_service, doc_id, requests)

    # ------------------------------------------------------------------
    # Pass 2 — add ToC links using heading IDs from fetched document
    # ------------------------------------------------------------------
    doc_data    = docs_service.documents().get(documentId=doc_id).execute()
    heading_ids = {}
    for elem in doc_data.get('body', {}).get('content', []):
        para = elem.get('paragraph', {})
        hid  = para.get('paragraphStyle', {}).get('headingId')
        if hid:
            text = ''.join(
                e.get('textRun', {}).get('content', '')
                for e in para.get('elements', [])
            ).strip()
            heading_ids[text] = hid

    toc_reqs = []
    for seg in segments:
        if seg['type'] == 'toc_entry':
            heading = features[seg['feature_idx']][1]
            hid     = heading_ids.get(heading)
            if hid:
                url = f'https://docs.google.com/document/d/{doc_id}/edit#heading={hid}'
                toc_reqs.append({
                    'updateTextStyle': {
                        'range': {
                            'startIndex': seg['start'] + 2,   # skip '• '
                            'endIndex':   seg['end']   - 1,   # skip '\n'
                        },
                        'textStyle': {'link': {'url': url}},
                        'fields': 'link',
                    }
                })
    if toc_reqs:
        docs_batch_update(docs_service, doc_id, toc_reqs)

    # ------------------------------------------------------------------
    # Pass 3 — insert NEXT_PAGE section breaks before section headers
    # and each feature heading.
    # IMPORTANT: must happen before Pass 3.5 (marker deletion) so that
    # segment positions from Pass 1 are still valid here.
    # ------------------------------------------------------------------
    break_reqs = [
        {'insertSectionBreak': {'location': {'index': seg['start']}, 'sectionType': 'NEXT_PAGE'}}
        for seg in reversed([s for s in segments if s['type'] in ('section_header', 'feature_heading')])
    ]
    if break_reqs:
        docs_batch_update(docs_service, doc_id, break_reqs)

    # ------------------------------------------------------------------
    # Pass 3.5 — apply hyperlinks from LNKS{n}Z / LNKE{n}Z markers,
    # then delete the markers. Runs after Pass 3 so it uses a fresh
    # doc.get() and is unaffected by the section break position shifts.
    # ------------------------------------------------------------------
    global_link_map = {}
    for feat in features:
        for n, url in feat[4].items():
            global_link_map[n] = url

    if global_link_map:
        doc_for_links = docs_service.documents().get(documentId=doc_id).execute()
        link_markers  = find_link_markers_in_doc(doc_for_links)

        # Apply hyperlink styles first (no position shifts yet)
        link_style_reqs = []
        for n, positions in link_markers.items():
            if 'start_marker' not in positions or 'end_marker' not in positions:
                continue
            url = global_link_map.get(n)
            if not url:
                continue
            sm_s, sm_e = positions['start_marker']
            em_s, _    = positions['end_marker']
            if sm_e < em_s:
                link_style_reqs.append({
                    'updateTextStyle': {
                        'range': {'startIndex': sm_e, 'endIndex': em_s},
                        'textStyle': {'link': {'url': url}},
                        'fields': 'link',
                    }
                })
        if link_style_reqs:
            docs_batch_update(docs_service, doc_id, link_style_reqs)

        # Delete all markers in one batched call (reverse order = safe)
        all_marker_ranges = []
        for positions in link_markers.values():
            if 'start_marker' in positions:
                all_marker_ranges.append(positions['start_marker'])
            if 'end_marker' in positions:
                all_marker_ranges.append(positions['end_marker'])
        all_marker_ranges.sort(key=lambda x: x[0], reverse=True)
        if all_marker_ranges:
            del_reqs = [{'deleteContentRange': {'range': {'startIndex': ms, 'endIndex': me}}}
                        for ms, me in all_marker_ranges]
            docs_batch_update(docs_service, doc_id, del_reqs)

    # ------------------------------------------------------------------
    # Pass 4 — insert inline images
    # Find IMG_PLACEHOLDER_N in the doc, replace with actual images
    # ------------------------------------------------------------------
    # Build a map from placeholder name → image info
    ph_to_info = {}
    for feat in features:
        _, _, _, img_map, _, inline_atts, msg_id, _ = feat
        for ph, cid in img_map.items():
            if cid in inline_atts:
                ph_to_info[ph] = {
                    'att_info': inline_atts[cid],
                    'msg_id':   msg_id,
                }

    if ph_to_info:
        doc_for_imgs = docs_service.documents().get(documentId=doc_id).execute()
        ph_positions = find_placeholders_in_doc(doc_for_imgs)

        # Sort by position descending so earlier indices are unaffected
        ordered = sorted(
            [(ph, pos) for ph, pos in ph_positions.items() if ph in ph_to_info],
            key=lambda x: x[1][0],
            reverse=True
        )

        for ph, (ph_start, ph_end) in ordered:
            info      = ph_to_info[ph]
            att_info  = info['att_info']
            msg_id    = info['msg_id']
            mime_type = att_info['mime_type']
            filename  = att_info['filename']

            print(f'  Downloading: {filename}')
            img_bytes = download_image(gmail_service, msg_id, att_info)
            if not img_bytes:
                print(f'  -> Could not download {filename}, skipping')
                continue

            print(f'  Uploading to GCS: {filename}')
            try:
                img_url = upload_image_to_gcs(storage_client, img_bytes, filename, mime_type)
                print(f'  -> Public URL: {img_url}')
            except Exception as e:
                print(f'  -> GCS upload failed ({e}), skipping image')
                docs_batch_update(docs_service, doc_id, [{'replaceAllText': {
                    'containsText': {'text': ph, 'matchCase': True},
                    'replaceText':  f'[Image: {filename}]',
                }}])
                continue

            w_pt, h_pt = image_size_pt(img_bytes, mime_type)
            try:
                # Phase 1: insert image BEFORE the placeholder + center its paragraph
                docs_batch_update(docs_service, doc_id, [
                    {
                        'insertInlineImage': {
                            'location':   {'index': ph_start},
                            'uri':        img_url,
                            'objectSize': {
                                'height': {'magnitude': h_pt, 'unit': 'PT'},
                                'width':  {'magnitude': w_pt, 'unit': 'PT'},
                            }
                        }
                    },
                    {
                        'updateParagraphStyle': {
                            'range': {
                                'startIndex': ph_start,
                                'endIndex':   ph_start + 1,
                            },
                            'paragraphStyle': {
                                'alignment':  'CENTER',
                                'spaceAbove': {'magnitude': 12, 'unit': 'PT'},
                                'spaceBelow': {'magnitude': 12, 'unit': 'PT'},
                            },
                            'fields': 'alignment,spaceAbove,spaceBelow',
                        }
                    },
                ])
                # Phase 2: delete the placeholder (now shifted right by 1)
                docs_batch_update(docs_service, doc_id, [{
                    'deleteContentRange': {
                        'range': {'startIndex': ph_start + 1, 'endIndex': ph_end + 1}
                    }
                }])
                print(f'  -> Inserted {w_pt:.0f}×{h_pt:.0f}pt')
            except Exception as e:
                print(f'  -> Image insert failed ({e}), replacing with text')
                docs_batch_update(docs_service, doc_id, [{'replaceAllText': {
                    'containsText': {'text': ph, 'matchCase': True},
                    'replaceText':  f'[Image: {filename}]',
                }}])

    doc_url = f'https://docs.google.com/document/d/{doc_id}/edit'
    return doc_id, doc_url


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(year=2026, month=2):
    month_name = calendar.month_name[month]
    doc_title  = f'Feature Announcements - {month_name} {year}'

    print('Authenticating...')
    creds = get_credentials()

    gmail          = build('gmail', 'v1', credentials=creds)
    docs           = build('docs',  'v1', credentials=creds)
    storage_client = gcs_lib.Client(project=GCS_PROJECT, credentials=creds)

    profile    = gmail.users().getProfile(userId='me').execute()
    user_email = profile.get('emailAddress', '')
    print(f'Authenticated as: {user_email}\n')

    first_day = f'{year}/{month:02d}/01'
    ny, nm    = (year + 1, 1) if month == 12 else (year, month + 1)
    last_day  = f'{ny}/{nm:02d}/01'

    # Search for GA and Preview emails separately, then combine
    searches = [
        ('[GA]',      'General Availability'),
        ('[Preview]', 'Preview'),
    ]

    features        = []
    global_img_ctr  = 0
    global_link_ctr = 0

    for tag, section_name in searches:
        query = (
            f'subject:"[Feature Announcement]" subject:"{tag}" '
            f'after:{first_day} before:{last_day}'
        )
        print(f'Searching Gmail ({section_name}): {query}')
        results  = gmail.users().messages().list(userId='me', q=query, maxResults=100).execute()
        messages = results.get('messages', [])
        print(f'Found {len(messages)} matching email(s)\n')

        for msg_ref in messages:
            msg = gmail.users().messages().get(
                userId='me', id=msg_ref['id'], format='full'
            ).execute()

            headers = msg.get('payload', {}).get('headers', [])
            subject = next(
                (h['value'] for h in headers if h['name'].lower() == 'subject'), ''
            )
            print(f'Processing: {subject}')

            if '[Feature Announcement]' not in subject or tag not in subject:
                print('  -> Missing required tags, skipping\n')
                continue

            body, img_map, link_map, inline_atts = get_email_body_and_images(msg)

            # Re-number image placeholders globally to avoid collisions across emails
            # Re-number image placeholders globally.
            # Use regex with (?!\d) to avoid IMG_PLACEHOLDER_1 matching inside
            # IMG_PLACEHOLDER_10, IMG_PLACEHOLDER_11, etc.
            new_img_map = {}
            img_n_mapping = {}
            for old_ph, cid in img_map.items():
                old_n = int(old_ph.split('_')[-1])
                new_ph = f'IMG_PLACEHOLDER_{global_img_ctr}'
                img_n_mapping[old_n] = global_img_ctr
                global_img_ctr += 1
                new_img_map[new_ph] = cid
            if img_n_mapping:
                def _remap_img(m):
                    n = int(m.group(1))
                    return f'IMG_PLACEHOLDER_{img_n_mapping[n]}' if n in img_n_mapping else m.group(0)
                body = re.sub(r'IMG_PLACEHOLDER_(\d+)', _remap_img, body)

            # Re-number link markers globally in a single regex pass to avoid
            # collisions from iterative str.replace (e.g. LNKS3Z→LNKS0Z then
            # LNKS0Z→LNKS3Z reverting the first replacement).
            new_link_map = {}
            link_n_mapping = {}
            for old_n in sorted(link_map.keys()):
                new_n = global_link_ctr
                global_link_ctr += 1
                link_n_mapping[old_n] = new_n
                new_link_map[new_n] = link_map[old_n]
            if link_n_mapping:
                def _remap_link(m):
                    n = int(m.group(2))
                    new_n = link_n_mapping.get(n)
                    if new_n is None:
                        return m.group(0)
                    return f'LNKS{new_n}Z' if m.group(1) == 'S' else f'LNKE{new_n}Z'
                body = re.sub(r'LNK([SE])(\d+)Z', _remap_link, body)

            release_note = extract_release_note(body)
            if not release_note:
                print('  -> No external release note section found, skipping\n')
                continue

            toc_entry, feature_heading = parse_subject(subject)
            cleaned   = clean_content(release_note, feature_heading)
            img_count = sum(1 for ph in new_img_map if ph in cleaned)
            print(f'  -> {len(cleaned)} chars, {img_count} image(s) [{section_name}]\n')

            features.append((
                toc_entry, feature_heading, cleaned,
                new_img_map, new_link_map, inline_atts, msg_ref['id'], section_name
            ))

    if not features:
        print('No qualifying features found. No document created.')
        return

    print('Fetching events and summits from events.datadoghq.com...')
    upcoming_events, on_demand_summits = fetch_summits()
    print(f'  {len(upcoming_events)} upcoming event(s), {len(on_demand_summits)} on-demand summit(s)\n')

    print('Fetching blog posts (last 3 months)...')
    blog_posts = fetch_blog_posts(months_back=3)

    print('Fetching training / enablement sessions...')
    training_sessions = fetch_training_sessions()

    print(f'Creating Google Doc: "{doc_title}"')
    _, doc_url = create_google_doc(
        docs, gmail, storage_client, doc_title, features,
        upcoming_events=upcoming_events,
        on_demand_summits=on_demand_summits,
        blog_posts=blog_posts,
        training_sessions=training_sessions,
    )

    print(f'\nDone! {len(features)} feature(s) documented.')
    print(f'Open your doc:\n  {doc_url}')


if __name__ == '__main__':
    main(year=2026, month=2)
