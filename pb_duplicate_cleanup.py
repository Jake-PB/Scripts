#!/usr/bin/env python3
"""
pb_duplicate_cleanup_v3.py
---------------------------
Cleans up duplicate company records in Productboard.

Intakes one CSV with columns: domain, uuid_with_origin
  - uuid_with_origin contains all company UUIDs for that domain tagged with
    their source, e.g.: {66c80e99-... (manual_or_csv), df3a7053-... (salesforce)}
  - Exactly 1 (salesforce) UUID  → processed
  - 0 (salesforce) UUIDs         → row skipped, logged
  - 2+ (salesforce) UUIDs        → row skipped, uuid_with_origin logged

For each non-Salesforce company UUID in a processed row:
  1. POST /v2/notes/search  — find all notes linked to the duplicate company
  2. POST /v2/entities/{sf_id}/relationships  — link SF company to:
       user UUID   if the note's customer relationship is type "user"
       note UUID   if the note's customer relationship is type "company"
  3. DELETE /companies/{dup_id}  — remove the duplicate (once per UUID)

Usage
-----
  python pb_duplicate_cleanup_v3.py companies.csv --token TOKEN        # dry-run
  python pb_duplicate_cleanup_v3.py companies.csv --token TOKEN --live # execute
  python pb_duplicate_cleanup_v3.py companies.csv --token TOKEN --live --log out.json
"""

import argparse
import csv
import dataclasses
import json
import logging
import re
import sys
import time
from dataclasses import dataclass
from typing import Optional

import requests

BASE_URL      = "https://api.productboard.com"
BASE_URL_V2   = "https://api.productboard.com/v2"
REQUEST_DELAY = 0.3

_SF_UUID_RE  = re.compile(
    r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\s*\(salesforce\)',
    re.IGNORECASE,
)
_ANY_UUID_RE = re.compile(
    r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})',
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class DomainRecord:
    domain: str
    sf_company_id: str
    duplicate_ids: list[str]   # non-SF company UUIDs to relink and delete


@dataclass
class ActionResult:
    domain: str
    note_uuid: str
    duplicate_company_id: str
    sf_company_id: str
    relationship_target_id: Optional[str] = None
    relationship_target_type: Optional[str] = None   # "user" or "note"
    create_rel_status: Optional[int] = None
    create_rel_ok: Optional[bool] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# CSV loader
# ---------------------------------------------------------------------------

def load_companies_csv(filepath: str) -> tuple[list[DomainRecord], list[dict]]:
    """
    Parse the companies CSV. Returns (domain_records, skipped_rows).
    Rows with 0 or 2+ Salesforce UUIDs are skipped and captured in skipped_rows.
    """
    domain_records: list[DomainRecord] = []
    skipped_rows: list[dict] = []

    with open(filepath, newline="", encoding="utf-8-sig") as fh:
        sample = fh.read(2048)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(fh, dialect=dialect)
        cols = {c.lower(): c for c in (reader.fieldnames or [])}
        for req in ("domain", "uuid_with_origin"):
            if req not in cols:
                raise ValueError(
                    f"Companies CSV missing '{req}' column. "
                    f"Found: {', '.join(reader.fieldnames or [])}"
                )

        for row in reader:
            domain = row[cols["domain"]].strip().lower()
            raw    = row[cols["uuid_with_origin"]].strip()
            if not domain:
                continue

            sf_uuids  = _SF_UUID_RE.findall(raw)
            all_uuids = _ANY_UUID_RE.findall(raw)
            sf_set    = {u.lower() for u in sf_uuids}
            dups      = [u for u in all_uuids if u.lower() not in sf_set]

            if len(sf_uuids) == 1:
                domain_records.append(DomainRecord(domain, sf_uuids[0], dups))
                logging.info("  domain='%s'  sf=%s  dups=%s", domain, sf_uuids[0], dups)
            elif len(sf_uuids) == 0:
                logging.warning("No SF UUID for domain '%s' — skipping. raw=%s", domain, raw)
                skipped_rows.append({"domain": domain, "uuid_with_origin": raw,
                                     "reason": "no_salesforce_uuid"})
            else:
                logging.warning(
                    "%d SF UUIDs for domain '%s' — skipping. raw=%s", len(sf_uuids), domain, raw
                )
                skipped_rows.append({"domain": domain, "uuid_with_origin": raw,
                                     "reason": "multiple_salesforce_uuids",
                                     "sf_uuids": sf_uuids})

    logging.info("CSV: %d domain(s) ready, %d skipped", len(domain_records), len(skipped_rows))
    return domain_records, skipped_rows


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _headers(token: str, v1: bool = False) -> dict:
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if v1:
        h["X-Version"] = "1"
    return h


def _req(session: requests.Session, method: str, url: str, token: str,
         v1: bool = False, **kwargs) -> requests.Response:
    """Make an HTTP request and enforce the rate-limit delay."""
    resp = getattr(session, method)(url, headers=_headers(token, v1), **kwargs)
    time.sleep(REQUEST_DELAY)
    return resp


def search_notes(session: requests.Session, token: str,
                 company_id: str, dry_run: bool) -> Optional[list[dict]]:
    """
    POST /v2/notes/search filtered by company UUID.
    Returns the full list of notes (paginated), or None on API error.
    """
    url     = f"{BASE_URL_V2}/notes/search"
    payload = {"data": {"relationships": {"customer": {"ids": [company_id]}}}}

    if dry_run:
        logging.info("[DRY-RUN] POST %s  body=%s", url, json.dumps(payload))
        return []

    resp = _req(session, "post", url, token, json=payload)
    logging.info("POST notes/search company=%s → %d", company_id, resp.status_code)

    if resp.status_code != 200:
        logging.warning("  body: %s", resp.text)
        return None

    notes: list[dict] = []
    body     = resp.json()
    notes.extend(body.get("data", []))
    next_url = body.get("links", {}).get("next")

    while next_url:
        resp = _req(session, "get", next_url, token)
        logging.info("  page → %d  (%d notes so far)", resp.status_code, len(notes))
        if resp.status_code != 200:
            logging.warning("  pagination failed: %s", resp.text)
            break
        body = resp.json()
        notes.extend(body.get("data", []))
        next_url = body.get("links", {}).get("next")

    logging.info("  %d note(s) found for company %s", len(notes), company_id)
    return notes


def delete_company(session: requests.Session, token: str,
                   company_id: str, dry_run: bool) -> tuple[Optional[int], bool]:
    url = f"{BASE_URL}/companies/{company_id}"
    if dry_run:
        logging.info("[DRY-RUN] DELETE %s", url)
        return None, True
    resp = _req(session, "delete", url, token, v1=True)
    ok   = resp.status_code in (200, 204)
    logging.info("DELETE company %s → %d", company_id, resp.status_code)
    if not ok:
        logging.warning("  body: %s", resp.text)
    return resp.status_code, ok


def set_user_parent_company(session: requests.Session, token: str,
                            user_uuid: str, sf_company_uuid: str,
                            dry_run: bool) -> tuple[Optional[int], bool]:
    """
    PUT /v2/entities/{user_uuid}/relationships/parent
    Sets the Salesforce company as the parent company of the user.
    """
    url     = f"{BASE_URL_V2}/entities/{user_uuid}/relationships/parent"
    payload = {"data": {"target": {"id": sf_company_uuid}, "type": "company"}}
    if dry_run:
        logging.info("[DRY-RUN] PUT %s  body=%s", url, json.dumps(payload))
        return None, True
    resp = _req(session, "put", url, token, json=payload)
    ok   = resp.status_code in (200, 201)
    logging.info("PUT user %s → parent company %s  HTTP %d", user_uuid, sf_company_uuid, resp.status_code)
    if not ok:
        logging.warning("  body: %s", resp.text)
    return resp.status_code, ok


def set_note_customer_company(session: requests.Session, token: str,
                              note_uuid: str, sf_company_uuid: str,
                              dry_run: bool) -> tuple[Optional[int], bool]:
    """
    PUT /v2/notes/{note_uuid}/relationships/customer
    Sets the Salesforce company as the customer on the note.
    """
    url     = f"{BASE_URL_V2}/notes/{note_uuid}/relationships/customer"
    payload = {"data": {"target": {"type": "company", "id": sf_company_uuid}}}
    if dry_run:
        logging.info("[DRY-RUN] PUT %s  body=%s", url, json.dumps(payload))
        return None, True
    resp = _req(session, "put", url, token, json=payload)
    ok   = resp.status_code in (200, 201)
    logging.info("PUT note %s → customer company %s  HTTP %d", note_uuid, sf_company_uuid, resp.status_code)
    if not ok:
        logging.warning("  body: %s", resp.text)
    return resp.status_code, ok


def resolve_target(note: dict) -> tuple[str, str]:
    """Return (target_id, target_type) for the relationship to create."""
    note_id = note.get("id", "")
    for rel in note.get("relationships", {}).get("data", []):
        if rel.get("type") == "customer":
            target = rel.get("target", {})
            if target.get("type") == "user":
                return target.get("id", note_id), "user"
            if target.get("type") == "company":
                return note_id, "note"
    logging.warning("Note %s has no customer relationship — using note UUID", note_id)
    return note_id, "note"


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def process(domain_records: list[DomainRecord], token: str,
            dry_run: bool) -> list[ActionResult]:
    session = requests.Session()
    results: list[ActionResult] = []
    deleted: set[str] = set()

    for dr in domain_records:
        logging.info("domain=%s  sf=%s  dups=%s", dr.domain, dr.sf_company_id, dr.duplicate_ids)

        for dup_id in dr.duplicate_ids:
            # 1 – Find notes linked to this duplicate company
            notes = search_notes(session, token, dup_id, dry_run)
            if notes is None:
                results.append(ActionResult(
                    domain=dr.domain, note_uuid="", duplicate_company_id=dup_id,
                    sf_company_id=dr.sf_company_id, error="notes search failed",
                ))
                continue

            # 2 – Relink each note to the Salesforce company.
            # Track every failure — if any relink fails, the company is NOT deleted.
            relink_failed = False
            for note in notes:
                note_id   = note.get("id", "")
                target_id, target_type = resolve_target(note)
                result = ActionResult(
                    domain=dr.domain,
                    note_uuid=note_id,
                    duplicate_company_id=dup_id,
                    sf_company_id=dr.sf_company_id,
                    relationship_target_id=target_id,
                    relationship_target_type=target_type,
                )
                try:
                    if target_type == "user":
                        result.create_rel_status, result.create_rel_ok = set_user_parent_company(
                            session, token, target_id, dr.sf_company_id, dry_run
                        )
                    else:  # "note"
                        result.create_rel_status, result.create_rel_ok = set_note_customer_company(
                            session, token, note_id, dr.sf_company_id, dry_run
                        )
                    if not result.create_rel_ok:
                        failed_id = target_id if target_type == "user" else note_id
                        result.error = f"relink failed (HTTP {result.create_rel_status})"
                        logging.warning(
                            "  Relink failed for %s %s (note=%s) — "
                            "company %s will NOT be deleted.",
                            target_type, failed_id, note_id, dup_id,
                        )
                        relink_failed = True
                except requests.RequestException as exc:
                    result.error = f"Network error: {exc}"
                    logging.warning(
                        "  Network error relinking %s %s (note=%s): %s — "
                        "company %s will NOT be deleted.",
                        target_type, target_id, note_id, exc, dup_id,
                    )
                    relink_failed = True
                results.append(result)

            # 3 – Delete the duplicate company only if every relink succeeded.
            if relink_failed:
                logging.warning(
                    "Skipping DELETE for company %s — one or more relinks failed above.",
                    dup_id,
                )
            elif dup_id not in deleted:
                _, ok = delete_company(session, token, dup_id, dry_run)
                if ok:
                    deleted.add(dup_id)

    return results


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_summary(results: list[ActionResult], skipped_rows: list[dict],
                  dry_run: bool) -> None:
    mode = "DRY-RUN PREVIEW" if dry_run else "LIVE EXECUTION"
    print(f"\n{'=' * 60}")
    print(f"  {mode} – {len(results)} action(s)")
    print(f"{'=' * 60}")

    for r in results:
        status = "✓ OK" if not r.error else f"✗ FAILED ({r.error})"
        target = (
            f"{r.relationship_target_type}: {r.relationship_target_id}"
            if r.relationship_target_id else "(dry-run)" if dry_run else "n/a"
        )
        print(
            f"  {status}\n"
            f"    domain            : {r.domain}\n"
            f"    note              : {r.note_uuid or '(none)'}\n"
            f"    duplicate company : {r.duplicate_company_id}\n"
            f"    sf company        : {r.sf_company_id}\n"
            f"    linked to         : {target}\n"
            f"    relationship ok   : {'yes' if r.create_rel_ok else 'no' if r.note_uuid else 'n/a'}\n"
        )

    if skipped_rows:
        print(f"{'─' * 60}")
        print(f"  Skipped rows ({len(skipped_rows)}):")
        for s in skipped_rows:
            print(f"    [{s['reason']}]  domain={s['domain']}  {s['uuid_with_origin']}")

    n_fail = sum(1 for r in results if r.error)
    print(f"{'─' * 60}")
    print(f"  OK: {len(results) - n_fail}   Failed: {n_fail}   Skipped rows: {len(skipped_rows)}")
    if dry_run:
        print("\n  ⚠  DRY-RUN — pass --live to execute for real.\n")
    else:
        print()


def save_log(results: list[ActionResult], skipped_rows: list[dict], filepath: str) -> None:
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump({
            "actions":         [dataclasses.asdict(r) for r in results],
            "skipped_domains": skipped_rows,
        }, fh, indent=2)
    logging.info("Log written to %s", filepath)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Relink notes from duplicate Productboard companies to their Salesforce equivalent, then delete the duplicates."
    )
    ap.add_argument("companies_csv", help="CSV with domain and uuid_with_origin columns")
    ap.add_argument("--token",   required=True, help="Productboard API bearer token")
    ap.add_argument("--live",    action="store_true", help="Execute for real (default is dry-run)")
    ap.add_argument("--log",     metavar="FILE",      help="Write JSON action log to FILE")
    ap.add_argument("--verbose", action="store_true", help="Show DEBUG-level output")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    dry_run = not args.live
    if dry_run:
        logging.info("DRY-RUN mode — pass --live to execute")

    try:
        domain_records, skipped_rows = load_companies_csv(args.companies_csv)
    except (FileNotFoundError, ValueError) as exc:
        logging.error("Input error: %s", exc)
        sys.exit(1)

    results = process(domain_records, token=args.token, dry_run=dry_run)
    print_summary(results, skipped_rows, dry_run)

    if args.log:
        save_log(results, skipped_rows, args.log)


if __name__ == "__main__":
    main()
