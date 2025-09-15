import json
import os
from typing import Dict, Any


# ------------ Flatten & Detection Logic ---------------- #

def flatten_record(record: Dict[str, Any]) -> Dict[str, str]:
    """Recursively flatten recordElements, recordExtensions, and recordSubExtensions into a key-value dict."""
    flat = {}

    def walk(node: Any):
        if isinstance(node, dict):
            # existing support for varName/varValue style
            if "varName" in node and "varValue" in node:
                flat[node["varName"]] = node["varValue"]

            # If there's an explicit recordElements container that is a dict, pull its primitive entries
            if "recordElements" in node:
                re = node["recordElements"]
                if isinstance(re, dict):
                    for k, v in re.items():
                        if isinstance(v, (str, int, float, bool)) or v is None:
                            flat[k] = "" if v is None else str(v)
                        else:
                            walk(v)
                elif isinstance(re, list):
                    for elem in re:
                        walk(elem)

            # Recurse into known containers (lists) and into other dict values
            for key in ("recordExtensions", "recordSubExtensions"):
                if key in node and isinstance(node[key], list):
                    for elem in node[key]:
                        walk(elem)

            # Also capture plain primitive key:value pairs found at this dict level
            for k, v in node.items():
                if k in ("recordElements", "recordExtensions", "recordSubExtensions", "varName", "varValue"):
                    continue
                if isinstance(v, (str, int, float, bool)) or v is None:
                    # keep existing values; later occurrences may overwrite but that's acceptable
                    flat[k] = "" if v is None else str(v)
                elif isinstance(v, (dict, list)):
                    # already handled some containers above, but ensure we walk other nested dicts/lists
                    walk(v)

        elif isinstance(node, list):
            for elem in node:
                walk(elem)

    walk(record)
    return flat


def detect_cdr_type(flat: Dict[str, str]) -> str:
    """Classify CDR type into SMS, VOICE, MMS, USSD, DATA, ECOMMERCE, or UNKNOWN."""
    svc_id = flat.get("serviceContextId", "").lower()
    evt = flat.get("recordEventType", "").upper()
    sub_evt = flat.get("subRecordEventType", "").upper()
    apn = flat.get("accessPointName", "")
    rat = flat.get("rATType", "")
    charging_service = flat.get("chargingServiceName", "").lower()
    tariff_id = flat.get("tariffID", "").lower()

    record_id = flat.get("recordId", "UNKNOWN_ID")

    # --- Debug print ---
    print(
        f"[DEBUG] recordId={record_id}, evt={evt}, sub_evt={sub_evt}, "
        f"svc_id={svc_id}, charging_service={charging_service}, "
        f"tariff_id={tariff_id}, flattened_keys={len(flat)}"
    )

    # --- DATA ---
    if evt == "PS" or "data" in svc_id or apn or rat in {"6", "7", "8"} or "tp_base_data" in charging_service:
        return "DATA"

    # --- MMS ---
    if "mms" in svc_id or evt == "MMS" or "mms" in charging_service:
        return "MMS"

    # --- VOICE ---
    if evt == "VOICE" or flat.get("mediaName", "").lower() == "speech":
        return "VOICE"

    # --- USSD ---
    if (
        "ussd" in svc_id
        or sub_evt == "USSD"
        or "ussd" in charging_service
        or "ussd" in tariff_id
    ):
        return "USSD"

    # --- SMS ---
    if (
        "sms" in svc_id
        or evt == "SMS"
        or "sms" in charging_service
        or "sms" in tariff_id
    ):
        return "SMS"

    # --- E-COMMERCE ---
    if "ecommerce" in svc_id or "payment" in charging_service or "ecom" in charging_service:
        return "ECOMMERCE"

    # Fallback: some update/notification records have no mscc block but are still data-related.
    # If serviceContextId matches OCS and we have APN / UE value / MCCMNC, treat as DATA.
    if svc_id.startswith("32251") and (apn or flat.get("userEquipmentValue") or flat.get("MCCMNC")):
        return "DATA"

    return "UNKNOWN"


# ------------ Processing Logic ---------------- #

def process_and_sort_cdr(input_file: str, output_dir: str):
    """Read CDR JSON file, detect type per record, and write to service folders."""
    with open(input_file, "r", encoding="utf-8") as f:
        cdr_data = json.load(f)

    # Normalize "records" which may be either a list (original expectation) or a dict (your file)
    records_obj = cdr_data.get("records", {})
    if isinstance(records_obj, list):
        entries = records_obj
    elif isinstance(records_obj, dict):
        # convert { "record1": {...}, "record2": {...} } -> [ {"record1": {...}}, {"record2": {...}} ]
        entries = [{k: v} for k, v in records_obj.items()]
    else:
        print("Unexpected 'records' format; expected list or dict")
        return

    for rec in entries:
        if not isinstance(rec, dict):
            continue
        # each entry is expected to be a single-key dict { "recordName": recordBody }
        rec_name, rec_body = next(iter(rec.items()))

        payload = rec_body.get("payload", {})
        if not payload:
            continue

        generic = payload.get("genericRecord")
        # Support both list-style genericRecord (original script) and dict-style (your file)
        if isinstance(generic, list) and len(generic) >= 2:
            record_content = generic[1]
        elif isinstance(generic, dict):
            record_content = generic
        else:
            # nothing usable
            continue

        # Flatten both payload and record_content
        flat = flatten_record(payload)
        flat.update(flatten_record(record_content))

        record_id = flat.get("recordId", rec_name)
        cdr_type = detect_cdr_type(flat)

        # Choose folder
        folder = cdr_type if cdr_type != "UNKNOWN" else "MISC"
        folder_path = os.path.join(output_dir, folder)
        os.makedirs(folder_path, exist_ok=True)

        # Save full record with header + payload intact
        out_file = os.path.join(folder_path, f"{record_id}.json")
        with open(out_file, "w", encoding="utf-8") as out_f:
            json.dump({rec_name: rec_body}, out_f, indent=2)

        print(f"Saved {record_id} → {cdr_type} folder")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python cdr_sorter.py <cdr_file.json> <output_dir>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    process_and_sort_cdr(input_file, output_dir)
