#!/usr/bin/env python3
"""
Production-ready CDR conversion + routing pipeline (config-driven).

- Reads config.json for successResultCodes, routingRules, circleRanges, dwhStructure, outputDirs, pollInterval.
- Computes EL_* fields according to spec.
- Routes files per routingRules (conditions) and DWH special structure (group_usage/single_usage + EL_REC_TYPE subfolders).
- Atomic writes, logging, graceful shutdown, robust JSON traversal.
"""

import json
import logging
import shutil
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import re

# -----------------------
# Paths & defaults
# -----------------------
CONFIG_PATH = Path("config1.json")
INPUT_DIR = Path("./in")
PROCESSED_DIR = Path("./processed")
LOG_DIR = Path("./logs")
DEFAULT_OUTPUT_DIRS = {
    "CRM": Path("./out/crm"),
    "BILLING": Path("./out/billing"),
    "LMS": Path("./out/lms"),
    "RA": Path("./out/ra"),
    "DWH": Path("./out/DWH"),
}
DEFAULT_CONFIG = {
    "pollInterval": 5,
    "successResultCodes": ["2001", "4012"],
    "circleRanges": {
        "0-9": 1, "10-19": 2, "20-29": 3, "30-39": 4,
        "40-49": 5, "50-59": 6, "60-69": 7, "70-79": 8,
        "80-89": 9, "90-99": 10
    },
    "dwhStructure": {
        "group_usage": ["DATA", "VOICE", "SMS", "MMS", "USSD", "ECOMMERCE"],
        "single_usage": ["DATA", "VOICE", "SMS", "MMS", "USSD", "ECOMMERCE"]
    },
    "outputDirs": {k: str(v) for k, v in DEFAULT_OUTPUT_DIRS.items()},
    "routingRules": [
        {"name": "CRM", "conditions": {}},
        {"name": "BILLING", "conditions": {"EL_PRE_POST": "POSTPAID", "EL_SUCCESS": 1}},
        {"name": "LMS", "conditions": {"EL_SUCCESS": 1, "EL_DEBIT_AMOUNT": ">0"}},
        {"name": "RA", "conditions": {}},
        {"name": "DWH", "conditions": {}}
    ]
}

# -----------------------
# Logging
# -----------------------
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    filename=str(LOG_DIR / "pipeline.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
logging.getLogger("").addHandler(console)
logging.info("Pipeline starting...")

# -----------------------
# Config loader
# -----------------------
def load_config() -> Dict[str, Any]:
    cfg = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r") as f:
                user_cfg = json.load(f)
            # Merge shallow keys
            for k, v in user_cfg.items():
                cfg[k] = v
            # Ensure outputDirs exist in config
            if "outputDirs" not in cfg:
                cfg["outputDirs"] = DEFAULT_CONFIG["outputDirs"]
        except Exception as e:
            logging.error(f"Failed to read config.json: {e}. Using defaults.")
    else:
        logging.warning("config.json not found. Using defaults.")
    return cfg

# -----------------------
# Generic traversal helpers
# -----------------------
def find_nodes_by_property(container: Any, prop_name: str) -> List[Dict[str, Any]]:
    """Recursively find nodes where recordProperty == prop_name."""
    found = []
    if isinstance(container, dict):
        if container.get("recordProperty") == prop_name:
            found.append(container)
        for k, v in container.items():
            if isinstance(v, (list, dict)):
                found.extend(find_nodes_by_property(v, prop_name))
    elif isinstance(container, list):
        for item in container:
            found.extend(find_nodes_by_property(item, prop_name))
    return found

def extract_record_elements(node: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(node, dict):
        return {}
    return node.get("recordElements", {}) or {}

def parse_generation_timestamp(ts: str) -> Tuple[str, str, str]:
    # Expect dd/mm/YYYY ... -> return YYYY, MM, DD
    try:
        if not ts or not isinstance(ts, str):
            return "0000", "00", "00"
        date_part = ts.strip().split(" ")[0]
        d, m, y = date_part.split("/")
        if len(y) == 2:
            y = "20" + y
        return y.zfill(4), m.zfill(2), d.zfill(2)
    except Exception:
        return "0000", "00", "00"

def get_circle_id_from_msisdn(msisdn: str, circle_ranges: Dict[str, int]) -> int:
    try:
        last_two = int(str(msisdn)[-2:])
    except Exception:
        return 0
    for r, val in circle_ranges.items():
        parts = r.split("-")
        if len(parts) != 2:
            continue
        try:
            s, e = int(parts[0]), int(parts[1])
        except:
            continue
        if s <= last_two <= e:
            return int(val)
    return 0

# -----------------------
# Build subscription info structures
# -----------------------
def find_subscription_info_nodes(root_generic: Dict[str, Any]) -> List[Dict[str, Any]]:
    return find_nodes_by_property(root_generic, "subscriptionInfo")

def find_subscription_id_elements(root_generic: Dict[str, Any]) -> List[Dict[str, Any]]:
    nodes = find_nodes_by_property(root_generic, "subscriptionId")
    elems = []
    for n in nodes:
        elems.append(extract_record_elements(n))
    return elems

def find_mscc_nodes(root_generic: Dict[str, Any]) -> List[Dict[str, Any]]:
    return find_nodes_by_property(root_generic, "mscc")

def get_charging_service_info_from_subscription_node(sub_node: Dict[str, Any]) -> Dict[str, Any]:
    result = {"chargingServiceElements": {}, "accountInfo": None, "bucketInfo": None}
    if not isinstance(sub_node, dict):
        return result
    for ext in sub_node.get("recordSubExtensions", []) or []:
        if ext.get("recordProperty") == "chargingServiceInfo":
            result["chargingServiceElements"] = extract_record_elements(ext)
            for c in ext.get("recordSubExtensions", []) or []:
                rp = c.get("recordProperty", "")
                if rp == "accountInfo":
                    result["accountInfo"] = extract_record_elements(c)
                elif rp == "bucketInfo":
                    result["bucketInfo"] = extract_record_elements(c)
            break
    # include subscription-level recordElements
    result["subscriptionRecordElements"] = extract_record_elements(sub_node)
    return result

# -----------------------
# EL field computation
# -----------------------
def compute_el_fields(generic_record: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute EL_* fields per specification.
    generic_record expected to be the 'genericRecord' node (may include 'recordElements' and nested 'recordExtensions').
    """
    el: Dict[str, Any] = {}
    root_elements = generic_record.get("recordElements", {}) if isinstance(generic_record, dict) else {}
    subscription_nodes = find_subscription_info_nodes(generic_record)
    subscription_infos = [get_charging_service_info_from_subscription_node(n) for n in subscription_nodes]
    subscription_id_elements = find_subscription_id_elements(generic_record)
    mscc_nodes = find_mscc_nodes(generic_record)

    # 1. EL_PRE_POST
    el_pre_post = None
    for s in subscription_infos:
        bucket = s.get("bucketInfo")
        account = s.get("accountInfo")
        if bucket:
            continue
        if account:
            el_pre_post = account.get("accountType") or account.get("accounttype") or account.get("AccountType")
            break
    if not el_pre_post:
        el_pre_post = root_elements.get("AccountType") or root_elements.get("accountType") or None
    if isinstance(el_pre_post, str) and el_pre_post.strip().upper() in ("PRE_PAID", "PREPAID", "PRE-PAID"):
        el["EL_PRE_POST"] = "PREPAID"
    elif isinstance(el_pre_post, str) and "POST" in el_pre_post.strip().upper():
        el["EL_PRE_POST"] = "POSTPAID"
    else:
        el["EL_PRE_POST"] = "UNKNOWN"

    # 2. EL_SUCCESS
    result_code = root_elements.get("resultCode") or root_elements.get("resultcode") or ""
    success_codes = set(map(str, cfg.get("successResultCodes", ["2001", "4012"])))
    el["EL_SUCCESS"] = 1 if str(result_code) in success_codes else 0

    # 3. EL_CUST_CARE
    el["EL_CUST_CARE"] = 1

    # 4. EL_BILLING
    el["EL_BILLING"] = 1 if el["EL_PRE_POST"] == "POSTPAID" and el["EL_SUCCESS"] == 1 else 0

    # 5. EL_REC_TYPE
    rec_map = {"PS": "DATA", "USSD": "USSD", "SMS": "SMS", "MMS": "MMS", "IMS": "VOICE", "ECOMMERCE": "ECOMMERCE"}
    record_event_type = ""
    if mscc_nodes:
        mscc_elem = extract_record_elements(mscc_nodes[0])
        record_event_type = mscc_elem.get("recordEventType") or mscc_elem.get("recordeventtype") or ""
    if not record_event_type:
        record_event_type = root_elements.get("recordEventType") or ""
    sub_record_type = root_elements.get("subRecordEventType") or ""
    rtype = (record_event_type or sub_record_type or "").strip().upper()
    el["EL_REC_TYPE"] = rec_map.get(rtype, "UNKNOWN")

    # 6. EL_EVENT_LABEL_VAL
    mapping = {
        "PS": 46, "MOC": 1, "FWD": 1, "MTC": 2,
        "SMS_MO": 25, "SMS_MT": 26, "USSD": 823, "MMS_MO": 143
    }
    if str(record_event_type).strip().upper() == "ECOMMERCE":
        service_type = (root_elements.get("ServiceType") or "").strip().upper()
        ecommerce_map = {
            "ACTIVATION": 824, "RECURRING_CHARGING": 825, "PAYMENT": 826,
            "SENDSMS": 827, "REQUESTSENDSMS": 828, "INTERACTIVESMS": 829
        }
        el["EL_EVENT_LABEL_VAL"] = ecommerce_map.get(service_type, 0)
    else:
        st = (sub_record_type or "").strip().upper()
        el["EL_EVENT_LABEL_VAL"] = mapping.get(st, 0)

    # 7. EL_DATE_DAY
    y, m, d = parse_generation_timestamp(root_elements.get("generationTimestamp") or "")
    el["EL_DATE_DAY"] = f"{y}{m}{d}" if y != "0000" else "00000000"

    # 8. EL_MONTH_VAL_CIRCLE_ID
    month = m if m else "00"
    msisdn = None
    # prefer types "1","103","0" (common variations) and extract bare digits if prefixed
    for sid in subscription_id_elements:
        sid_type = str(sid.get("subscriptionIdType", "")).strip()
        sid_data = sid.get("subscriptionIdData", "") or ""
        if sid_type in ("1", "103", "0"):
            cand = sid_data.split("-", 1)[-1] if "-" in sid_data else sid_data
            if isinstance(cand, str) and re.search(r"\d{6,}$", cand):
                msisdn = cand
                break
    if not msisdn:
        uname = root_elements.get("userName") or ""
        match = re.search(r"(\d{6,})", uname)
        if match:
            msisdn = match.group(1)
    circle_id = get_circle_id_from_msisdn(msisdn or "", cfg.get("circleRanges", {}))
    el["EL_MONTH_VAL_CIRCLE_ID"] = f"{month}_{circle_id}"

    # 9. EL_DEBIT_AMOUNT
    debit_amount = 0.0
    found = False
    for s in subscription_infos:
        account = s.get("accountInfo")
        bucket = s.get("bucketInfo")
        if not bucket and account:
            # assign accountBalanceCommitted
            try:
                debit_amount = float(account.get("accountBalanceCommitted", 0) or 0)
            except:
                debit_amount = float(str(account.get("accountBalanceCommitted", "0")).strip() or 0)
            found = True
            break
        elif account:
            try:
                before = float(account.get("accountBalanceBefore", 0) or 0)
                after = float(account.get("accountBalanceAfter", 0) or 0)
                debit_amount = before - after
            except:
                try:
                    before = float(str(account.get("accountBalanceBefore", "0")).strip() or 0)
                    after = float(str(account.get("accountBalanceAfter", "0")).strip() or 0)
                    debit_amount = before - after
                except:
                    debit_amount = 0.0
            found = True
            break
    el["EL_DEBIT_AMOUNT"] = float(debit_amount)

    # 10. EL_GROUP_USAGE
    group_nodes = find_nodes_by_property(generic_record, "groupInfo")
    group_active = False
    for g in group_nodes:
        ge = extract_record_elements(g)
        gs = ge.get("groupState") or ""
        if isinstance(gs, str) and gs.strip().upper() == "ACTIVE":
            group_active = True
            break
    el["EL_GROUP_USAGE"] = 1 if group_active else 0

    return el

# -----------------------
# Condition evaluator (used for routing)
# -----------------------
def evaluate_condition(record_fields: Dict[str, Any], key: str, condition: Any) -> bool:
    """
    Evaluate a single condition against record_fields.
    Supports strings like '>0', '>=1', '<=10', '!=X', '==Y' and direct equality for strings/numbers.
    If key missing, returns False.
    """
    if key not in record_fields:
        return False
    val = record_fields[key]
    # attempt numeric conversion
    try:
        val_num = float(val)
    except Exception:
        val_num = None

    # If condition is numeric or bool, compare directly
    if isinstance(condition, (int, float)):
        if val_num is None:
            return False
        return float(val_num) == float(condition)

    if isinstance(condition, bool):
        return bool(val) == condition

    if isinstance(condition, str):
        cond = condition.strip()
        for op in (">=", "<=", ">", "<", "!=", "=="):
            if cond.startswith(op):
                rhs = cond[len(op):].strip()
                # try numeric comparison
                try:
                    rhs_num = float(rhs)
                    if val_num is None:
                        return False
                    if op == ">":
                        return val_num > rhs_num
                    if op == "<":
                        return val_num < rhs_num
                    if op == ">=":
                        return val_num >= rhs_num
                    if op == "<=":
                        return val_num <= rhs_num
                    if op == "==":
                        return val_num == rhs_num
                    if op == "!=":
                        return val_num != rhs_num
                except:
                    # string comparison
                    if op == "==":
                        return str(val) == rhs
                    if op == "!=":
                        return str(val) != rhs
                    return False
        # no operator: direct string compare (case sensitive)
        return str(val) == cond

    # fallback strict equality
    return val == condition

# -----------------------
# File I/O helpers & routing
# -----------------------
def atomic_write_json(path: Path, obj: Any):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    tmp.replace(path)

def ensure_output_dirs(cfg: Dict[str, Any]):
    out_dirs = cfg.get("outputDirs", DEFAULT_CONFIG["outputDirs"])
    for name, p in out_dirs.items():
        Path(p).mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def route_using_config(original_obj: Dict[str, Any], el_fields: Dict[str, Any], cfg: Dict[str, Any], input_filename: str):
    """
    Use cfg['routingRules'] to route files. If a rule has empty conditions -> route all records.
    DWH gets special structure: group_usage/single_usage + EL_REC_TYPE subfolder.
    Also enforce LMS exclusion for ECOMMERCE (per spec).
    """
    out_dirs = cfg.get("outputDirs", DEFAULT_CONFIG["outputDirs"])
    routing_rules = cfg.get("routingRules", DEFAULT_CONFIG["routingRules"])

    # attach CBL_TAG to payload wrapper for output
    out_obj = {"original": original_obj, "CBL_TAG": el_fields}

    for rule in routing_rules:
        name = rule.get("name")
        conditions = rule.get("conditions", {}) or {}
        # evaluate all conditions
        match = True
        for k, cond in conditions.items():
            if not evaluate_condition(el_fields, k, cond):
                match = False
                break
        if not match:
            continue  # skip this rule

        # Special handling for DWH
        if name.upper() == "DWH":
            base = Path(out_dirs.get("DWH", DEFAULT_OUTPUT_DIRS["DWH"]))
            category = "group_usage" if el_fields.get("EL_GROUP_USAGE") == 1 else "single_usage"
            rec_type = (el_fields.get("EL_REC_TYPE") or "OTHER")
            final_dir = base / category / str(rec_type)
            final_dir.mkdir(parents=True, exist_ok=True)
            atomic_write_json(final_dir / input_filename, out_obj)
            logging.info(f"Routed {input_filename} -> DWH ({category}/{rec_type})")
            continue

        # Special handling: LMS exclusion for ECOMMERCE (spec requirement).
        if name.upper() == "LMS":
            # route only if not ECOMMERCE in EL_REC_TYPE even if conditions matched
            if (el_fields.get("EL_REC_TYPE") or "").upper() == "ECOMMERCE":
                logging.info(f"Skipping LMS routing for {input_filename} because EL_REC_TYPE is ECOMMERCE")
                continue

        # Normal write for other destinations
        dest_dir = Path(out_dirs.get(name, f"./out/{name.lower()}"))
        dest_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(dest_dir / input_filename, out_obj)
        logging.info(f"Routed {input_filename} -> {name}")

# -----------------------
# Process a single file
# -----------------------
def process_file(path: Path, cfg: Dict[str, Any]):
    fn = path.name
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception as e:
        logging.error(f"Failed to parse JSON {fn}: {e}")
        # move invalid file to processed to avoid retry loop
        try:
            shutil.move(str(path), str(PROCESSED_DIR / fn))
        except Exception as e2:
            logging.error(f"Failed to move invalid file {fn}: {e2}")
        return

    # Locate genericRecord robustly
    generic = None
    try:
        generic = (data.get("payload", {}) or {}).get("genericRecord") or (data.get("record2", {}) or {}).get("payload", {}).get("genericRecord")
    except Exception:
        generic = None
    if not generic:
        # fallback: if data looks like recordElements at root
        if "payload" in data and isinstance(data["payload"], dict) and "genericRecord" in data["payload"]:
            generic = data["payload"]["genericRecord"]
        else:
            generic = data.get("genericRecord") or {"recordElements": data.get("recordElements", {})}

    el_fields = compute_el_fields(generic, cfg)
    # route according to config
    ensure_output_dirs(cfg)
    # Wrap original into object so outputs carry EL metadata
    route_using_config(data, el_fields, cfg, fn)

    # move processed file to processed dir
    try:
        dest = PROCESSED_DIR / fn
        shutil.move(str(path), str(dest))
    except Exception as e:
        logging.error(f"Failed to move processed file {fn}: {e}")

# -----------------------
# Main loop with graceful shutdown
# -----------------------
STOP = False

def _signal_handler(sig, frame):
    global STOP
    logging.info("Shutdown requested.")
    STOP = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

def main():
    cfg = load_config()
    poll = int(cfg.get("pollInterval", DEFAULT_CONFIG["pollInterval"]))
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    logging.info(f"Watching input directory: {INPUT_DIR}")
    while not STOP:
        try:
            files = sorted([p for p in INPUT_DIR.glob("*.json") if p.is_file()])
            if not files:
                time.sleep(poll)
                continue
            for p in files:
                if STOP:
                    break
                logging.info(f"Processing file: {p.name}")
                try:
                    process_file(p, cfg)
                except Exception as e:
                    logging.exception(f"Unhandled error processing {p.name}: {e}")
        except Exception as e:
            logging.exception(f"Main loop error: {e}")
            time.sleep(poll)
    logging.info("Pipeline stopped gracefully.")

if __name__ == "__main__":
    main()
