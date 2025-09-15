import asn1tools
import json
import os
import logging
from pathlib import Path
from typing import Any, Dict, List, Union

# -------------------------------
# CONFIG
# -------------------------------
INPUT_FOLDER = "in"
OUTPUT_FOLDER = "out"
LOG_FOLDER = "log"
ASN1_SCHEMA_FILE = "description.asn"
TOP_LEVEL_TYPE = "SPSRecord"

# Metadata template (can be customized per-record/file)
DEFAULT_METADATA = [
    "#input_id 1756361674x001_0011141",
    "#output_id",
    "#input_type OCSChargingRecord",
    "#output_type OCSChargingRecord",
    "#addkey",
    "#source_id LOCAL_CLTR",
    "#filename example.CDR.gz"
]

# -------------------------------
# SETUP LOGGING
# -------------------------------
Path(LOG_FOLDER).mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(Path(LOG_FOLDER) / "decode.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# -------------------------------
# HELPERS
# -------------------------------
def decode_all_ber_records(raw_data: bytes, spec: Any) -> List[Dict[str, Any]]:
    """Decode all BER records and return structured records."""
    records: List[Dict[str, Any]] = []
    offset = 0
    total_len = len(raw_data)
    record_count = 1
    valid_tags = {'a0', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'bf8148', '31'}

    while offset < total_len:
        slice_data = raw_data[offset:]
        if not slice_data:
            break
        try:
            # Prefer decode_with_length if supported
            if hasattr(spec, "decode_with_length"):
                decoded, consumed_len = spec.decode_with_length(TOP_LEVEL_TYPE, slice_data)
                record_len = consumed_len
            else:
                decoded = spec.decode(TOP_LEVEL_TYPE, slice_data)
                encoded_bytes = spec.encode(TOP_LEVEL_TYPE, decoded)
                record_len = len(encoded_bytes)

            records.append({
                f"record{record_count}": {
                    "header": {"recordLength": record_len},
                    "payload": {"genericRecord": decoded}
                }
            })

            offset += record_len
            record_count += 1
        except Exception as e:
            first_byte = f"{slice_data[0]:02x}"
            if first_byte not in valid_tags:
                logging.debug(f"Skipping unknown tag {first_byte} at offset {offset}")
                offset += 1
            else:
                logging.warning(f"Decoding failed at offset {offset}: {e}")
                offset += 1

    return records


def convert_record(record: Dict[str, Any], metadata: List[str] | None = None) -> str:
    """Convert record JSON into custom text format."""
    lines = ["RECORD"]
    lines += metadata if metadata else DEFAULT_METADATA

    record_key = next(iter(record.keys()), None)
    if not record_key:
        return "\n".join(lines)

    payload = (
        record.get(record_key, {})
        .get("payload", {})
        .get("genericRecord")
    )

    # Handle tuple/list case
    if isinstance(payload, (list, tuple)) and len(payload) > 1:
        payload = payload[1]
    if not isinstance(payload, dict):
        logging.warning("Unexpected payload format")
        return "\n".join(lines)

    def process_elements(elements: List[Dict[str, Any]]) -> List[str]:
        return [
            f"F {elem['varName']} {elem['varValue']}"
            for elem in elements
            if "varName" in elem and "varValue" in elem
        ]

    def process_subextensions(subexts: List[Dict[str, Any]]) -> List[str]:
        result: List[str] = []
        for sub in subexts:
            prop = sub.get("recordProperty", "UNKNOWN")
            result.append(f"B {prop}")
            result += process_elements(sub.get("recordElements", []))
            nested_subs = sub.get("recordSubExtensions", [])
            if nested_subs:
                result += process_subextensions(nested_subs)
            result.append(".")
        return result

    record_elements = payload.get("recordElements", [])
    lines += process_elements(record_elements)

    record_extensions = payload.get("recordExtensions", [])
    for ext in record_extensions:
        lines.append(f"B {ext.get('recordProperty', 'UNKNOWN')}")
        subexts = ext.get("recordSubExtensions", [])
        lines += process_subextensions(subexts)
        lines.append(".")

    return "\n".join(lines)


# ---------- Flattening utilities ----------

def _is_var_pair(d: Any) -> bool:
    """True if dict is exactly {'varName': ..., 'varValue': ...}."""
    return isinstance(d, dict) and set(d.keys()) == {"varName", "varValue"}

def _flatten_var_pair(d: Dict[str, Any]) -> Dict[str, Any]:
    """Convert {'varName': X, 'varValue': Y} -> {X: Y}."""
    return {str(d["varName"]): d["varValue"]}

def _collapse_single_key_dicts(lst: List[Any]) -> Union[List[Any], Dict[str, Any]]:
    """
    If 'lst' looks like a list of single-key dicts, merge into one dict.
    If duplicate keys occur, accumulate values into a list.
    Otherwise, return the original (possibly transformed) list.
    """
    if not isinstance(lst, list) or not lst:
        return lst

    # Only proceed if every element is a dict with exactly one key
    if not all(isinstance(x, dict) and len(x) == 1 for x in lst):
        return lst

    merged: Dict[str, Any] = {}
    for d in lst:
        (k, v), = d.items()
        if k in merged:
            # Turn into list if duplicate key
            if not isinstance(merged[k], list):
                merged[k] = [merged[k]]
            merged[k].append(v)
        else:
            merged[k] = v
    return merged

def _untag_known_pairs(obj: Any) -> Any:
    """
    Some decoders produce tagged pairs like ["genericRecord", {...}].
    If we see a two-element list where the first is a string and the second is a dict,
    return just the transformed dict (dropping the tag).
    """
    if isinstance(obj, (list, tuple)) and len(obj) == 2 and isinstance(obj[0], str) and isinstance(obj[1], dict):
        return obj[1]
    return obj

def flatten_and_collapse(obj: Any) -> Any:
    """
    Recursively:
      1) Flatten {'varName': X, 'varValue': Y} -> {X: Y}
      2) Collapse lists of single-key dicts into one dict
      3) Handle common tagged pairs like ["genericRecord", {...}] -> {...}
      4) Apply (2) especially for keys like 'recordElements'
    """
    # First, handle tagged pairs
    obj = _untag_known_pairs(obj)

    # Flatten var-pairs directly
    if _is_var_pair(obj):
        return _flatten_var_pair(obj)

    # Recurse
    if isinstance(obj, dict):
        transformed = {k: flatten_and_collapse(v) for k, v in obj.items()}

        # Special-case common element containers
        for key in list(transformed.keys()):
            if key.endswith("Elements") or key == "recordElements":
                transformed[key] = _collapse_single_key_dicts(
                    transformed[key] if isinstance(transformed[key], list) else transformed[key]
                )
            elif key.endswith("List") or key.endswith("Records"):
                # No special action; present for future extension
                pass

        return transformed

    if isinstance(obj, list):
        lst = [flatten_and_collapse(v) for v in obj]
        # Try to collapse if it became a list of single-key dicts
        collapsed = _collapse_single_key_dicts(lst)
        return collapsed

    return obj

# -------------------------------
# MAIN
# -------------------------------
def main() -> None:
    Path(OUTPUT_FOLDER).mkdir(exist_ok=True)

    logging.info(f"Compiling ASN.1 schema: {ASN1_SCHEMA_FILE}")
    try:
        spec = asn1tools.compile_files(ASN1_SCHEMA_FILE, 'ber')
    except Exception as e:
        logging.error(f"Failed to compile ASN.1 schema: {e}")
        return

    cdr_files = list(Path(INPUT_FOLDER).glob("*.CDR"))
    if not cdr_files:
        logging.warning("No CDR files found in folder.")
        return

    for cdr_file in cdr_files:
        logging.info(f"Processing file: {cdr_file}")
        try:
            raw_data = cdr_file.read_bytes()
        except Exception as e:
            logging.error(f"Failed to read file {cdr_file}: {e}")
            continue

        decoded_records = decode_all_ber_records(raw_data, spec)
        if not decoded_records:
            logging.warning(f"No records decoded from {cdr_file}")
            continue

        # Save JSON output (original structure)
        output_json_path = Path(OUTPUT_FOLDER) / f"{cdr_file.stem}_decoded.json"
        output_data = {
            "header": {
                "fileLength": len(raw_data),
                "headerLength": 54
            },
            "records": decoded_records
        }
        try:
            with open(output_json_path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.error(f"Failed to save JSON for {cdr_file}: {e}")
            continue

        # Save flattened & collapsed JSON output
        output_flattened_path = Path(OUTPUT_FOLDER) / f"{cdr_file.stem}_flattened.json"
        flattened_records = flatten_and_collapse(decoded_records)
        flattened_data = {
            "header": output_data["header"],
            "records": flattened_records
        }
        try:
            with open(output_flattened_path, "w", encoding="utf-8") as f:
                json.dump(flattened_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.error(f"Failed to save flattened JSON for {cdr_file}: {e}")
            continue

        # Convert all records to text format (legacy)
        all_records_output = "\n".join(
            convert_record(r, metadata=[*DEFAULT_METADATA, f"#filename {cdr_file.name}"])
            for r in decoded_records
        )
        output_txt_path = Path(OUTPUT_FOLDER) / f"{cdr_file.stem}_converted.txt"
        try:
            with open(output_txt_path, "w", encoding="utf-8") as f:
                f.write(all_records_output)
        except Exception as e:
            logging.error(f"Failed to save converted text for {cdr_file}: {e}")
            continue

        logging.info(f"File processed successfully. Records decoded: {len(decoded_records)}")

    logging.info("All files processed.")


if __name__ == "__main__":
    main()
