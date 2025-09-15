import json
from types import SimpleNamespace
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Tuple
import logging
from logging.handlers import RotatingFileHandler
import sys
import shutil
import time
from datetime import datetime

# CONFIG
CONFIG = {
    "INPUT_PATH": "./cdr_input",
    "ARCHIVE_DIR": "./archive",
    "ACCEPTED_DIR": "./cdr_accepted",
    "REJECTED_DIR": "./cdr_rejected",
    "LOG_PATH": "validetion.log",
    "DEBUG": False,
    "DATA_RG": "100,200",
    "VOICE_RG": "",
    "SMS_RG": "",
    "STRICT_EL": False,
    "BILLING": False,
    "WATCH": True,
    "POLL_INTERVAL": 5
}

# ------------------- Utility Functions -------------------
def to_decimal(v) -> Decimal:
    try:
        if v is None or v == "":
            return Decimal(0)
        return Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


def walk_mscc_blocks(generic: Dict[str, Any]) -> List[Dict[str, Any]]:
    msccs = []
    for ext in generic.get("recordExtensions", []) or []:
        if ext.get("recordProperty") != "listOfMscc":
            continue
        for sub in ext.get("recordSubExtensions", []) or []:
            if sub.get("recordProperty") == "mscc":
                msccs.append(sub)
    return msccs


def has_block_anywhere(msccs: List[Dict[str, Any]], block_name: str) -> bool:
    def search_node(node: Any) -> bool:
        if isinstance(node, dict):
            if node.get("recordProperty") == block_name:
                return True
            for key in ("recordSubExtensions", "recordExtensions"):
                for child in node.get(key, []) or []:
                    if search_node(child):
                        return True
            for v in node.values():
                if isinstance(v, (dict, list)) and search_node(v):
                    return True
        elif isinstance(node, list):
            for elem in node:
                if search_node(elem):
                    return True
        return False

    for m in msccs:
        if search_node(m):
            return True
    return False


def extract_numeric_indicators(msccs: List[Dict[str, Any]]) -> Dict[str, Decimal]:
    totals = {
        "totalVolumeConsumed": Decimal(0),
        "totalUnitsConsumed": Decimal(0),
        "totalTimeConsumed": Decimal(0),
        "bucketCommitedUnits": Decimal(0),
        "accountBalanceCommitted": Decimal(0),
    }
    for m in msccs:
        re = m.get("recordElements", {}) or {}
        totals["totalVolumeConsumed"] += to_decimal(re.get("totalVolumeConsumed", 0))
        totals["totalUnitsConsumed"] += to_decimal(re.get("totalUnitsConsumed", 0))
        totals["totalTimeConsumed"] += to_decimal(re.get("totalTimeConsumed", 0))
        for dev in m.get("recordSubExtensions", []) or []:
            for sub in dev.get("recordSubExtensions", []) or []:
                if sub.get("recordProperty") == "subscriptionInfo":
                    for charging in sub.get("recordSubExtensions", []) or []:
                        if charging.get("recordProperty") == "chargingServiceInfo":
                            for cs_sub in charging.get("recordSubExtensions", []) or []:
                                if cs_sub.get("recordProperty") == "bucketInfo":
                                    b = cs_sub.get("recordElements", {}) or {}
                                    totals["bucketCommitedUnits"] += to_decimal(b.get("bucketCommitedUnits", 0))
                                if cs_sub.get("recordProperty") == "accountInfo":
                                    a = cs_sub.get("recordElements", {}) or {}
                                    totals["accountBalanceCommitted"] += to_decimal(a.get("accountBalanceCommitted", 0))
                                if cs_sub.get("recordProperty") == "noCharge":
                                    nc = cs_sub.get("recordElements", {}) or {}
                                    totals["bucketCommitedUnits"] += to_decimal(nc.get("noChargeCommittedUnits", 0))
    return totals


def detect_cdr_type_from_generic(generic: Dict[str, Any]) -> str:
    elems = generic.get("recordElements", {}) or {}
    svc_id = (elems.get("serviceContextId") or "").lower()
    evt = (elems.get("recordEventType") or "").upper()
    apn = elems.get("accessPointName", "")
    rat = elems.get("rATType", "")
    charging = ""
    for ext in generic.get("recordExtensions", []) or []:
        if ext.get("recordProperty") == "listOfMscc":
            for mscc in ext.get("recordSubExtensions", []) or []:
                for dev in mscc.get("recordSubExtensions", []) or []:
                    for sub in dev.get("recordSubExtensions", []) or []:
                        if sub.get("recordProperty") == "subscriptionInfo":
                            for ch in sub.get("recordSubExtensions", []) or []:
                                if ch.get("recordProperty") == "chargingServiceInfo":
                                    charging = (ch.get("recordElements", {}) or {}).get("chargingServiceName", "") or charging
    charging = (charging or "").lower()
    if evt == "PS" or "data" in svc_id or apn or str(rat) in {"6", "7", "8"} or "tp_base_data" in charging:
        return "DATA"
    if "mms" in svc_id or evt == "MMS" or "mms" in charging:
        return "MMS"
    if evt == "VOICE" or (elems.get("mediaName") or "").lower() == "speech":
        return "VOICE"
    if "ussd" in svc_id or (elems.get("subRecordEventType") or "").upper() == "USSD" or "ussd" in charging:
        return "USSD"
    if "sms" in svc_id or evt == "SMS" or "sms" in charging:
        return "SMS"
    if "ecommerce" in svc_id or "payment" in charging or "ecom" in charging:
        return "ECOMMERCE"
    return "UNKNOWN"

# ------------------- Filtration Rules -------------------
def apply_filtration_rules(record_name: str, generic: Dict[str, Any], strict_el: bool,
                           data_rg_whitelist: List[str], voice_rg_whitelist: List[str],
                           sms_rg_whitelist: List[str],
                           billing: bool = False) -> Tuple[bool, List[str]]:
    reasons = []
    rtype = generic.get("recordType", "")
    if rtype != "OCSChargingRecord":
        reasons.append("recordType != OCSChargingRecord")
        return False, reasons

    msccs = walk_mscc_blocks(generic)
    if not msccs:
        reasons.append("no listOfMscc block")
        return False, reasons

    if not (has_block_anywhere(msccs, "accountInfo") or has_block_anywhere(msccs, "bucketInfo") or
            has_block_anywhere(msccs, "additionalBalanceInfo") or has_block_anywhere(msccs, "groupInfo") or
            has_block_anywhere(msccs, "groupState")):
        reasons.append("missing accountInfo/bucketInfo/additionalBalanceInfo/groupInfo/groupState")
        return False, reasons

    totals = extract_numeric_indicators(msccs)
    cdr_type = detect_cdr_type_from_generic(generic)

    # Type-specific numeric filtration
    if cdr_type == "DATA":
        if totals["totalVolumeConsumed"] == 0 and totals["bucketCommitedUnits"] == 0 and totals["accountBalanceCommitted"] == 0:
            reasons.append("DATA: no volume, bucket units or account balance")
            return False, reasons
    elif cdr_type == "VOICE":
        if totals["totalTimeConsumed"] == 0 and totals["bucketCommitedUnits"] == 0 and totals["accountBalanceCommitted"] == 0:
            reasons.append("VOICE: no time, bucket units or account balance")
            return False, reasons
    elif cdr_type in {"SMS", "USSD", "MMS"}:
        if totals["totalUnitsConsumed"] == 0 and totals["bucketCommitedUnits"] == 0 and totals["accountBalanceCommitted"] == 0:
            reasons.append(f"{cdr_type}: no units, bucket units or account balance")
            return False, reasons

    # Billing filtration
    if billing:
        rating_groups = [str((m.get("recordElements", {}) or {}).get("ratingGroup")) for m in msccs if (m.get("recordElements", {}) or {}).get("ratingGroup")]
        if cdr_type == "DATA" and any(r in data_rg_whitelist for r in rating_groups):
            reasons.append(f"DATA ratingGroup in whitelist {rating_groups}")
            return False, reasons
        if cdr_type == "VOICE" and any(r in voice_rg_whitelist for r in rating_groups):
            reasons.append(f"VOICE ratingGroup in whitelist {rating_groups}")
            return False, reasons
        if cdr_type in {"SMS", "USSD", "MMS"} and any(r in sms_rg_whitelist for r in rating_groups):
            reasons.append(f"{cdr_type} ratingGroup in whitelist {rating_groups}")
            return False, reasons

        elems = generic.get("recordElements", {}) or {}
        el_success = elems.get("EL_SUCCESS") or elems.get("elSuccess") or elems.get("resultCode")
        el_pre_post = elems.get("EL_PRE_POST") or elems.get("el_pre_post") or elems.get("prePost")
        if strict_el:
            if el_success != 1 or str(el_pre_post).upper() != "POSTPAID":
                reasons.append("EL_SUCCESS/EL_PRE_POST check failed (strict)")
                return False, reasons
        else:
            if el_success is not None and el_pre_post is not None:
                if str(el_success) != "1" or str(el_pre_post).upper() != "POSTPAID":
                    reasons.append("EL_SUCCESS/EL_PRE_POST check failed")
                    return False, reasons

        for m in msccs:
            for dev in m.get("recordSubExtensions", []) or []:
                for sub in dev.get("recordSubExtensions", []) or []:
                    for ch in sub.get("recordSubExtensions", []) or []:
                        if ch.get("recordProperty") == "additionalBalanceInfo":
                            usage = (ch.get("recordElements", {}) or {}).get("usageType")
                            if usage and str(usage).upper() == "SECONDARY_BALANCE":
                                reasons.append("additionalBalanceInfo.usageType == SECONDARY_BALANCE")
                                return False, reasons

    return True, ["passed"]

# ------------------- Logging -------------------
def setup_logging(log_path: str | None, debug: bool = False):
    lvl = logging.DEBUG if debug else logging.INFO
    logger = logging.getLogger("validetion")
    logger.setLevel(lvl)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    if log_path:
        fh = RotatingFileHandler(log_path, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger

# ------------------- File Operations -------------------
def save_file(file: Path, accepted: bool, args):
    target_dir = Path(args.accepted_dir if accepted else args.rejected_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(file, target_dir / file.name)

def process_file(input_file: Path, args, logger) -> None:
    try:
        doc = json.loads(input_file.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to read/parse %s", input_file)
        save_file(input_file, False, args)
        return

    generics = []
    if "payload" in doc and "genericRecord" in doc["payload"]:
        generic = doc["payload"]["genericRecord"]
        generics = generic if isinstance(generic, list) else [generic]
    else:
        logger.error("Unrecognized format for file %s", input_file)
        save_file(input_file, False, args)
        return

    for idx, gen in enumerate(generics):
        record_id = (gen.get("recordElements", {}) or {}).get("recordId", f"{input_file.stem}_{idx}")
        keep, reasons = apply_filtration_rules(
            record_id,
            gen,
            args.strict_el,
            [x.strip() for x in args.data_rg.split(",") if x.strip()],
            [x.strip() for x in args.voice_rg.split(",") if x.strip()],
            [x.strip() for x in args.sms_rg.split(",") if x.strip()],
            billing=args.billing,
        )
        if keep:
            logger.info("ACCEPTED %s", record_id)
            save_file(input_file, True, args)
        else:
            logger.warning("REJECTED %s reasons=%s", record_id, reasons)
            save_file(input_file, False, args)

# ------------------- Directory Watch -------------------
def watch_directory(input_dir: Path, args, logger):
    archive_dir = Path(args.archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Watching directory %s (poll interval %s sec)", input_dir, args.poll_interval)

    while True:
        try:
            files = sorted([p for p in input_dir.iterdir() if p.is_file() and p.suffix.lower() in {".json", ".ndjson"}])
            for f in files:
                if time.time() - f.stat().st_mtime < args.poll_interval:
                    continue
                logger.info("Detected new stable file %s, processing...", f)
                process_file(f, args, logger)
                ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                dest = archive_dir / f"{f.stem}_{ts}{f.suffix}"
                try:
                    shutil.move(f, dest)
                    logger.info("Archived raw file %s -> %s", f.name, dest.name)
                except Exception:
                    logger.exception("Failed to archive %s; leaving file in place", f)
        except Exception:
            logger.exception("Error while watching directory")
        time.sleep(args.poll_interval)

# ------------------- Main -------------------
def main():
    cfg = CONFIG
    input_arg = sys.argv[1] if len(sys.argv) > 1 else None

    args = SimpleNamespace(
        input=input_arg or cfg["INPUT_PATH"],
        archive_dir=cfg["ARCHIVE_DIR"],
        accepted_dir=cfg["ACCEPTED_DIR"],
        rejected_dir=cfg["REJECTED_DIR"],
        log=cfg["LOG_PATH"],
        debug=cfg["DEBUG"],
        data_rg=cfg["DATA_RG"],
        voice_rg=cfg["VOICE_RG"],
        sms_rg=cfg["SMS_RG"],
        strict_el=cfg["STRICT_EL"],
        billing=cfg["BILLING"],
        watch=cfg["WATCH"],
        poll_interval=cfg["POLL_INTERVAL"]
    )

    logger = setup_logging(args.log, debug=args.debug)
    input_path = Path(args.input)
    Path(args.archive_dir).mkdir(parents=True, exist_ok=True)

    if args.watch:
        if not input_path.is_dir():
            logger.error("Watch mode requires a directory. %s is not a directory.", input_path)
            return
        watch_directory(input_path, args, logger)
        return

    # Single-run processing
    files = sorted([p for p in input_path.iterdir() if p.is_file() and p.suffix.lower() in {".json", ".ndjson"}]) if input_path.is_dir() else [input_path]
    logger.info("Starting processing %d file(s)", len(files))
    for f in files:
        process_file(f, args, logger)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        archive_path = Path(args.archive_dir) / f"{f.stem}_{ts}{f.suffix}"
        try:
            shutil.move(f, archive_path)
            logger.info("Archived raw file %s -> %s", f.name, archive_path.name)
        except Exception:
            logger.exception("Failed to archive %s; leaving file in place", f)

if __name__ == "__main__":
    main()
