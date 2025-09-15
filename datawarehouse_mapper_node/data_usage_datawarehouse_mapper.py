#!/usr/bin/env python3
"""
Table 8 ER mapper - production-ready script
Maps incoming CDR JSON files to the Table-8 ER layout and writes mapped JSONs.
Watches a folder for new files and processes any existing JSONs at startup.
"""

import json
import binascii
import re
import time
import logging
from decimal import Decimal, InvalidOperation
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from typing import List, Dict, Any, Optional

# ---------------- Config ----------------
WATCH_FOLDER = Path("./watch_folder")
OUTPUT_FOLDER = Path("./output_folder")
LOG_FILE = "script.log"

WATCH_FOLDER.mkdir(parents=True, exist_ok=True)
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ---------------- Helpers ----------------
def safe_get(d: dict, *keys):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
        if cur is None:
            return None
    return cur

def to_decimal(v) -> Optional[Decimal]:
    if v in (None, ""):
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        return None

def fmt_decimal(d: Optional[Decimal]) -> str:
    if d is None:
        return ""
    try:
        return format(d.quantize(Decimal("0.00000")), 'f')
    except Exception:
        return str(d)

def last_n_chars(s: Optional[str], n: int) -> str:
    if not s:
        return ""
    s = str(s).strip()
    return s[-n:] if len(s) >= n else s

def decode_location_hex_field(hexstr: Optional[str]) -> str:
    """
    Implements MCCMNC-TAC-eNodeB-CELL decoding for rATType==6 per the rule.
    If <18 chars then fallback to 14-char split (6-4-4)
    """
    if not hexstr:
        return ""
    s = last_n_chars(hexstr, 18)
    if len(s) < 18:
        s14 = last_n_chars(hexstr, 14)
        if not s14:
            return ""
        # split 6-4-4
        return f"{s14[-14:-8]}-{s14[-8:-4]}-{s14[-4:]}"
    tac_hex = s[0:4]
    mccmnc_hex = s[4:10]
    eci_hex = s[10:18]
    try:
        tac_dec = int(tac_hex, 16)
    except Exception:
        tac_dec = 0
    # nibble swap each byte pair for MCCMNC and remove F
    pairs = [mccmnc_hex[i:i+2] for i in range(0, len(mccmnc_hex), 2)]
    swapped = "".join(p[::-1] for p in pairs)
    swapped_clean = swapped.replace('F', '')
    mccmnc_val = swapped_clean
    try:
        eci_int = int(eci_hex, 16)
    except Exception:
        eci_int = 0
    enb = eci_int % 256
    cell = eci_int // 256
    return f"{mccmnc_val}-{tac_dec}-{enb}-{cell}"

def imei_from_user_equipment_value(hex_or_str: Optional[str]) -> str:
    """
    Form a 16-digit number using digits from even positions in userEquipmentValue
    index starting from 1 to 31. If hex provided, try to decode ascii first.
    """
    if not hex_or_str:
        return ""
    decoded = None
    try:
        b = binascii.unhexlify(hex_or_str)
        decoded = b.decode('ascii', errors='ignore')
        if not decoded:
            decoded = None
    except Exception:
        decoded = None
    src = decoded if decoded else str(hex_or_str)
    # take even positions (1-based) => indexes 1,3,5,... => (i+1)%2==0
    even_chars = "".join(ch for i, ch in enumerate(src) if ((i + 1) % 2) == 0)
    digits = re.sub(r'\D', '', even_chars)
    if len(digits) >= 16:
        return digits[:16]
    return digits.ljust(16, "0")

# ---------------- subscription / account / bucket helpers ----------------
def find_mscc_block(rec: dict) -> Optional[dict]:
    for ext in rec.get("recordExtensions", []) or []:
        if ext.get("recordProperty") == "listOfMscc":
            for sub in ext.get("recordSubExtensions", []) or []:
                if sub.get("recordProperty") == "mscc":
                    return sub
    return None

def collect_subscription_blocks(mscc: Optional[dict]) -> List[dict]:
    subs = []
    if not mscc:
        return subs
    for dev in mscc.get("recordSubExtensions", []) or []:
        if dev.get("recordProperty") != "deviceInfo":
            continue
        for subinfo in dev.get("recordSubExtensions", []) or []:
            if subinfo.get("recordProperty") != "subscriptionInfo":
                continue
            subs.append(subinfo)
    return subs

def extract_noCharge_values(subs: List[dict]) -> List[str]:
    vals = []
    for sub in subs:
        for charge in sub.get("recordSubExtensions", []) or []:
            if charge.get("recordProperty") != "chargingServiceInfo":
                continue
            for csub in charge.get("recordSubExtensions", []) or []:
                if csub.get("recordProperty") == "noCharge":
                    nc = csub.get("recordElements", {}) or {}
                    v = nc.get("noChargeCommittedUnits")
                    if v not in (None, ""):
                        vals.append(str(v))
    return vals

def extract_account_slots(subs: List[dict], max_slots: int = 5) -> List[dict]:
    slots = []
    for sub in subs:
        if len(slots) >= max_slots:
            break
        for charge in sub.get("recordSubExtensions", []) or []:
            if charge.get("recordProperty") != "chargingServiceInfo":
                continue
            for csub in charge.get("recordSubExtensions", []) or []:
                if csub.get("recordProperty") == "accountInfo":
                    acc = csub.get("recordElements", {}) or {}
                    # accountBalanceCommittedBR might be present under different key; map both
                    slots.append({
                        "accountID": acc.get("accountID", "") or "",
                        "accountType": acc.get("accountType", "") or "",
                        "accountBalanceAfter": acc.get("accountBalanceAfter", "") or "",
                        "accountBalanceBefore": acc.get("accountBalanceBefore", "") or "",
                        # some inputs use accountBalanceCommittedBR
                        "accountBalanceCommitted": acc.get("accountBalanceCommitted", acc.get("accountBalanceCommittedBR", "")) or "",
                        "secondaryCostCommitted": acc.get("secondaryCostCommitted", "") or "",
                        "rateId": acc.get("rateId", "") or "",
                        "committedTaxAmount": acc.get("committedTaxAmount", "") or "",
                        "totalVolumeCharged": acc.get("totalVolumeCharged", ""),
                        "roundedVolumeCharged": acc.get("roundedVolumeCharged", "")
                    })
                    break
            if len(slots) >= max_slots:
                break
    # pad
    while len(slots) < max_slots:
        slots.append({
            "accountID": "",
            "accountType": "",
            "accountBalanceAfter": "",
            "accountBalanceBefore": "",
            "accountBalanceCommitted": "",
            "secondaryCostCommitted": "",
            "rateId": "",
            "committedTaxAmount": "",
            "totalVolumeCharged": "",
            "roundedVolumeCharged": ""
        })
    return slots

def extract_bucket_slots(subs: List[dict], max_slots: int = 5) -> List[dict]:
    slots = []
    for sub in subs:
        if len(slots) >= max_slots:
            break
        bundle_name = safe_get(sub, "recordElements", "bundleName") or ""
        bucket_entries = []
        for charge in sub.get("recordSubExtensions", []) or []:
            if charge.get("recordProperty") != "chargingServiceInfo":
                continue
            for csub in charge.get("recordSubExtensions", []) or []:
                if csub.get("recordProperty") == "bucketInfo":
                    b = csub.get("recordElements", {}) or {}
                    bucket_entries.append({
                        "bucketName": b.get("bucketName", "") or "",
                        "bucketUnitType": b.get("bucketUnitType", "") or "",
                        "bucketBalanceAfter": b.get("bucketBalanceAfter", "") or "",
                        "bucketBalanceBefore": b.get("bucketBalanceBefore", "") or "",
                        "bucketCommitedUnits": b.get("bucketCommitedUnits", "") or "",
                        "rateId": b.get("rateId", "") or "",
                        "committedTaxAmount": b.get("committedTaxAmount", "") or ""
                    })
        if bucket_entries:
            joined_bucket_names = ",".join(
                (f"{bundle_name}-{e['bucketName']}" if bundle_name else e['bucketName']) for e in bucket_entries
            )
            joined_unit_types = ",".join(e["bucketUnitType"] for e in bucket_entries)
            joined_balance_after = ",".join(e["bucketBalanceAfter"] for e in bucket_entries)
            chg_list = []
            for e in bucket_entries:
                before = to_decimal(e["bucketBalanceBefore"])
                after = to_decimal(e["bucketBalanceAfter"])
                if before is not None and after is not None:
                    diff = before - after
                    if diff < 0:
                        chg_list.append(e["bucketCommitedUnits"] or "")
                    else:
                        chg_list.append(str(diff))
                else:
                    chg_list.append("")
            joined_chg = ",".join(chg_list)
            joined_rate_ids = ",".join(e["rateId"] for e in bucket_entries if e.get("rateId"))
            committed_tax = ""
            for e in bucket_entries:
                if e.get("committedTaxAmount"):
                    committed_tax = e.get("committedTaxAmount")
                    break
            slots.append({
                "bucket_balance_id": joined_bucket_names,
                "bucket_unit_type": joined_unit_types,
                "bucket_cur_balance": joined_balance_after,
                "bucket_chg_balance": joined_chg,
                "bucket_rate_id": joined_rate_ids,
                "committedTaxAmount": committed_tax
            })
    while len(slots) < max_slots:
        slots.append({
            "bucket_balance_id": "",
            "bucket_unit_type": "",
            "bucket_cur_balance": "",
            "bucket_chg_balance": "",
            "bucket_rate_id": "",
            "committedTaxAmount": ""
        })
    return slots

def extract_bundle_list(subs: List[dict]) -> List[str]:
    bundles = []
    for sub in subs:
        bn = safe_get(sub, "recordElements", "bundleName")
        if bn:
            bundles.append(bn)
    return bundles

def extract_alternate_ids(subs: List[dict]) -> List[str]:
    vals = []
    for sub in subs:
        alt = safe_get(sub, "recordElements", "alternateId")
        if alt:
            vals.append(str(alt))
    return vals

def extract_additional_balance_info(subs: List[dict]) -> List[dict]:
    """
    Collect additionalBalanceInfo entries across all subscriptionInfo blocks.
    Each entry includes chargingServiceName, usageType, usedAs and bucketInfo subfields.
    """
    out = []
    for sub in subs:
        for charge in sub.get("recordSubExtensions", []) or []:
            if charge.get("recordProperty") != "chargingServiceInfo":
                continue
            add = safe_get(charge, "recordElements", "additionalBalanceInfo")
            if not add:
                # additionalBalanceInfo may be in a nested recordSubExtensions block
                for nested in charge.get("recordSubExtensions", []) or []:
                    if nested.get("recordProperty") == "additionalBalanceInfo":
                        add = nested.get("recordElements", {}) or {}
                        break
            if add:
                # bucketInfo may be nested similarly
                bi = add.get("bucketInfo", {}) or {}
                out.append({
                    "chargingServiceName": add.get("chargingServiceName", "") or add.get("chargingServiceName", ""),
                    "usageType": add.get("usageType", "") or "",
                    "usedAs": add.get("usedAs", "") or "",
                    "bucketName": bi.get("bucketName", "") or "",
                    "bucketUnitType": bi.get("bucketUnitType", "") or "",
                    "bucketKindOfUnit": bi.get("bucketKindOfUnit", "") or "",
                    "bucketBalanceBefore": bi.get("bucketBalanceBefore", "") or "",
                    "bucketBalanceAfter": bi.get("bucketBalanceAfter", "") or "",
                    "carryOverBucket": bi.get("carryOverBucket", "") or "",
                    "bucketCommitedUnits": bi.get("bucketCommitedUnits", "") or "",
                    "bucketReservedUnits": bi.get("bucketReservedUnits", "") or "",
                    "rateId": bi.get("rateId", "") or "",
                    "primaryCostCommitted": bi.get("primaryCostCommitted", "") or "",
                    "secondaryCostCommitted": bi.get("secondaryCostCommitted", "") or "",
                    "taxationID": bi.get("taxationID", "") or "",
                    "taxRateApplied": bi.get("taxRateApplied", "") or "",
                    "committedTaxAmount": bi.get("committedTaxAmount", "") or "",
                    "totalTaxAmount": bi.get("totalTaxAmount", "") or "",
                    "tariffID": bi.get("tariffID", "") or "",
                    "totalVolumeCharged": bi.get("totalVolumeCharged", "") or "",
                    "roundedVolumeCharged": bi.get("roundedVolumeCharged", "") or "",
                    "deltaVolume": bi.get("deltaVolume", "") or ""
                })
    return out

# ---------------- subscription ID extract ----------------
def extract_subscription_ids_from_rec(rec: dict) -> (str, str):
    msisdn = ""
    imsi = ""
    for ext in rec.get("recordExtensions", []) or []:
        if ext.get("recordProperty") == "listOfSubscriptionID":
            for sub in ext.get("recordSubExtensions", []) or []:
                if sub.get("recordProperty") != "subscriptionId":
                    continue
                relem = sub.get("recordElements", {}) or {}
                sid = relem.get("subscriptionIdData", "") or ""
                stype = str(relem.get("subscriptionIdType", ""))
                if stype == "0" and not msisdn:
                    msisdn = sid
                if stype == "1" and not imsi:
                    imsi = sid
    return msisdn, imsi

# ---------------- Main mapping function: implements all 115 fields ----------------
def map_table8_full(raw: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}

    rec = safe_get(raw, "original", "payload", "genericRecord")
    if not rec:
        logging.warning("No genericRecord found in input.")
        return {}

    elems = rec.get("recordElements", {}) or {}
    cbl = raw.get("CBL_TAG", {}) or {}

    # find mscc block
    mscc_block = find_mscc_block(rec)
    mscc_elems = (mscc_block.get("recordElements", {}) or {}) if mscc_block else {}

    # basic direct mappings
    result["EL_CDR_ID"] = elems.get("sessionId", "")                          # 1
    result["EL_CDR_SUB_ID"] = mscc_elems.get("localSequenceNumber", "")       # 2
    result["EL_SRC_CDR_ID"] = ""                                              # 3 N.A.
    result["EL_CUST_LOCAL_START_DATE"] = elems.get("generationTimestamp", "") # 4
    result["EL_RATE_USAGE"] = mscc_elems.get("totalVolumeConsumed", mscc_elems.get("timeUsage", ""))  # 5

    # EL_DEBIT_AMOUNT rule (6)
    debit_val = None
    subs = collect_subscription_blocks(mscc_block)
    for sub in subs:
        for charge in sub.get("recordSubExtensions", []) or []:
            if charge.get("recordProperty") != "chargingServiceInfo":
                continue
            for csub in charge.get("recordSubExtensions", []) or []:
                if csub.get("recordProperty") == "accountInfo":
                    acc = csub.get("recordElements", {}) or {}
                    # priority: accountBalanceCommittedBR -> accountBalanceCommitted -> before-after
                    debit_val = to_decimal(acc.get("accountBalanceCommittedBR") or acc.get("accountBalanceCommitted"))
                    if debit_val is None:
                        before = to_decimal(acc.get("accountBalanceBefore"))
                        after = to_decimal(acc.get("accountBalanceAfter"))
                        if before is not None and after is not None:
                            debit_val = before - after
                    break
            if debit_val is not None:
                break
        if debit_val is not None:
            break
    result["EL_DEBIT_AMOUNT"] = fmt_decimal(debit_val)                        # 6

    result["EL_FREE_UNIT_AMOUNT_OF_DURATION"] = ""                            # 7 N.A.
    free_flux_vals = extract_noCharge_values(subs)
    result["EL_FREE_UNIT_AMOUNT_OF_FLUX"] = ",".join(free_flux_vals) if free_flux_vals else ""  # 8

    # Account slots 1..5 (9..33)
    acct_slots = extract_account_slots(subs, max_slots=5)
    for i, slot in enumerate(acct_slots, start=1):
        result[f"EL_ACCT_BALANCE_ID{i}"] = slot["accountID"]                  # 9,14,19,24,29
        result[f"EL_BALANCE_TYPE{i}"] = slot["accountType"]                   # 10,15,20,25,30
        result[f"EL_CUR_BALANCE{i}"] = slot["accountBalanceAfter"]            # 11,16,21,26,31

        before = to_decimal(slot["accountBalanceBefore"])
        after = to_decimal(slot["accountBalanceAfter"])
        committed = to_decimal(slot["accountBalanceCommitted"])
        secondary = to_decimal(slot["secondaryCostCommitted"])

        chg = None
        if before is not None and after is not None:
            diff = before - after
            if diff < 0:
                # if negative, map committed + secondary
                add = Decimal(0)
                if committed is not None:
                    add += committed
                if secondary is not None:
                    add += secondary
                chg = add
            else:
                chg = diff
        result[f"EL_CHG_BALANCE{i}"] = fmt_decimal(chg)                       # 12,17,22,27,32

        result[f"EL_RATE_ID{i}"] = slot.get("rateId", "")                      # 13,18,23,28,33

    # Buckets slots 1..5 (34..58)
    bucket_slots = extract_bucket_slots(subs, max_slots=5)
    for i, b in enumerate(bucket_slots, start=1):
        result[f"EL_BUCKET_BALANCE_ID{i}"] = b["bucket_balance_id"]           # 34,39,44,49,54
        result[f"EL_BUCKET_BALANCE_TYPE{i}"] = b["bucket_unit_type"]          # 35,40,45,50,55
        result[f"EL_BUCKET_CUR_BALANCE{i}"] = b["bucket_cur_balance"]         # 36,41,46,51,56
        result[f"EL_BUCKET_CHG_BALANCE{i}"] = b["bucket_chg_balance"]        # 37,42,47,52,57
        result[f"EL_BUCKET_RATE_ID{i}"] = b["bucket_rate_id"]                # 38,43,48,53,58

    # calling party and apn (59..66)
    msisdn, imsi = extract_subscription_ids_from_rec(rec)
    result["EL_CALLING_PARTY_NUMBER"] = msisdn                           # 59
    result["EL_APN"] = elems.get("accessPointName", "")                  # 60
    result["EL_URL"] = ""                                                # 61
    result["EL_CALLING_PARTY_IMSI"] = imsi                               # 62
    result["EL_TOTAL_FLUX"] = mscc_elems.get("totalVolumeConsumed", "")  # 63
    result["EL_UP_FLUX"] = mscc_elems.get("uplinkVolumeConsumed", "")    # 64
    result["EL_DOWN_FLUX"] = mscc_elems.get("downlinkVolumeConsumed", "")# 65
    result["EL_ELAPSE_DURATION"] = elems.get("duration", "")            # 66

    # IMEI (67)
    result["EL_IMEI"] = imei_from_user_equipment_value(elems.get("userEquipmentValue", ""))  # 67

    result["EL_BEARER_PROTOCOL_TYPE"] = ""                              # 68

    # main offering id (69)
    result["EL_MAIN_OFFERING_ID"] = ",".join(extract_bundle_list(subs)) if extract_bundle_list(subs) else ""  # 69

    # pay type (70) - from CBL_TAG
    result["EL_PAY_TYPE"] = cbl.get("EL_PRE_POST", "") or ""            # 70

    # charging type (71)
    charging_type = ""
    for sub in subs:
        for charge in sub.get("recordSubExtensions", []) or []:
            if charge.get("recordProperty") == "chargingServiceInfo":
                charging_type = safe_get(charge, "recordElements", "chargingServiceType") or ""
                if charging_type:
                    break
        if charging_type:
            break
    result["EL_CHARGING_TYPE"] = charging_type                          # 71

    result["EL_ROAM_STATE"] = elems.get("roamingIndicator", "")         # 72
    result["EL_CALLING_VPN_TOP_GROUP_NUMBER"] = ""                      # 73
    result["EL_CALLING_VPN_GROUP_NUMBER"] = ""                          # 74
    result["EL_START_TIME_OF_BILL_CYCLE"] = elems.get("recordOpeningTime", "")  # 75
    result["EL_LAST_EFFECT_OFFERING"] = ""                               # 76
    result["EL_RATING_GROUP"] = mscc_elems.get("ratingGroup", "")        # 77
    result["EL_USER_STATE"] = elems.get("deviceState", "")              # 78
    result["EL_RAT_TYPE"] = elems.get("rATType", "")                    # 79
    result["EL_CHARGE_PARTY_INDICATOR"] = ""                             # 80
    result["EL_COUNTRY_NAME"] = ""                                       # 81
    result["EL_PAY_DEFAULT_ACCT_ID"] = ""                                # 82

    # TAX1 from first account slot if present (83)
    acct_committed_tax = acct_slots[0].get("committedTaxAmount", "") if acct_slots else ""
    result["EL_TAX1"] = acct_committed_tax or ""                         # 83

    # TAX2 from first bucket if present (84)
    result["EL_TAX2"] = bucket_slots[0].get("committedTaxAmount", "") if bucket_slots else ""  # 84

    # LOCATION (85) - uses userLocationInformation and rATType
    result["EL_LOCATION"] = decode_location_hex_field(elems.get("userLocationInformation", "")) if str(elems.get("rATType", "")) == "6" else (
        (lambda s=last_n_chars(elems.get("userLocationInformation", ""), 14): (f"{s[-14:-8]}-{s[-8:-4]}-{s[-4:]}" if s else ""))()
    )

    # alternate ids (86)
    result["EL_ALTERNATE_ID"] = "~".join(extract_alternate_ids(subs)) if extract_alternate_ids(subs) else ""  # 86

    # fields 87..89
    result["EL_BUSINESS_TYPE"] = ""                                      # 87
    result["EL_SUBSCRIBER_KEY"] = ""                                    # 88
    result["EL_ACCOUNT_KEY"] = ""                                       # 89

    # Additional balance info fields 90..111 - iterate additionalBalanceInfo entries
    add_infos = extract_additional_balance_info(subs)
    # We'll collect each attribute as comma-joined lists in the order they appear
    def join_attr(key):
        return ",".join(a.get(key, "") for a in add_infos if a.get(key) not in (None, ""))

    result["EL_ADDITIONALBALANCEINFO_CHARGINGSERVICENAME"] = join_attr("chargingServiceName")  # 90
    result["EL_ADDITIONALBALANCEINFO_USAGETYPE"] = join_attr("usageType")                     # 91
    result["EL_ADDITIONALBALANCEINFO_USEDAS"] = join_attr("usedAs")                           # 92
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETNAME"] = join_attr("bucketName")         # 93
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETUNITTYPE"] = join_attr("bucketUnitType")  # 94
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETKINDOFUNIT"] = join_attr("bucketKindOfUnit")  # 95
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEBEFORE"] = join_attr("bucketBalanceBefore")  # 96
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEAFTER"] = join_attr("bucketBalanceAfter")    # 97
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_CARRYOVERBUCKET"] = join_attr("carryOverBucket")         # 98
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETCOMMITEDUNITS"] = join_attr("bucketCommitedUnits")  # 99
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETRESERVEDUNITS"] = join_attr("bucketReservedUnits")   # 100
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_RATEID"] = join_attr("rateId")                           # 101
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_PRIMARYCOSTCOMMITTED"] = join_attr("primaryCostCommitted") # 102
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_SECONDARYCOSTCOMMITTED"] = join_attr("secondaryCostCommitted") # 103
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXATIONID"] = join_attr("taxationID")                    # 104
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXRATEAPPLIED"] = join_attr("taxRateApplied")            # 105
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_COMMITTEDTAXAMOUNT"] = join_attr("committedTaxAmount")    # 106
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALTAXAMOUNT"] = join_attr("totalTaxAmount")            # 107
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TARIFFID"] = join_attr("tariffID")                        # 108
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALVOLUMECHARGED"] = join_attr("totalVolumeCharged")    # 109
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_ROUNDEDVOLUMECHARGED"] = join_attr("roundedVolumeCharged")# 110
    result["EL_ADDITIONALBALANCEINFO_BUCKETINFO_DELTAVOLUME"] = join_attr("deltaVolume")                  # 111

    # Unlimited bundle rules 112..114
    # If not present bucketInfo AND accountBalanceCommitted == 0 AND totalVolumeCharged > 0 then map
    unlimited_bundle_name = ""
    unlimited_total_volume_charged = ""
    unlimited_unit_type = ""
    for sub, acct in zip(subs, acct_slots):
        # check if there is bucketInfo under chargingServiceInfo for this subscription
        has_bucket = False
        for charge in sub.get("recordSubExtensions", []) or []:
            if charge.get("recordProperty") != "chargingServiceInfo":
                continue
            for csub in charge.get("recordSubExtensions", []) or []:
                if csub.get("recordProperty") == "bucketInfo":
                    has_bucket = True
                    break
            if has_bucket:
                break
        # accountBalanceCommitted value numeric?
        acct_committed = to_decimal(acct.get("accountBalanceCommitted"))
        total_vol = acct.get("totalVolumeCharged") or ""
        # if totalVolumeCharged not in account slot (maybe present in account record elements)
        if not total_vol:
            # attempt to find in account recordElements in subs; we already included fields in acct_slots
            total_vol = acct.get("totalVolumeCharged", "")

        # apply rule
        if (not has_bucket) and (acct_committed is not None and acct_committed == 0) and (total_vol not in (None, "", "0")):
            unlimited_bundle_name = safe_get(sub, "recordElements", "bundleName") or ""
            unlimited_total_volume_charged = total_vol
            unlimited_unit_type = "VOLUME"
            break

    result["EL_UNLTD_BUNDLE_NAME"] = unlimited_bundle_name                         # 112
    result["EL_UNLTD_TOTAL_VOLUME_CHARGED"] = unlimited_total_volume_charged        # 113
    result["EL_UNLTD_BUNDLE_UNIT_TYPE"] = unlimited_unit_type                      # 114

    # ORIG LOCATION 115 uses origUserLocationInfo similar to EL_LOCATION
    if str(elems.get("rATType", "")) == "6":
        result["EL_ORIG_LOCATION"] = decode_location_hex_field(elems.get("origUserLocationInfo", ""))
    else:
        s14 = last_n_chars(elems.get("origUserLocationInfo", ""), 14)
        if s14:
            result["EL_ORIG_LOCATION"] = f"{s14[-14:-8]}-{s14[-8:-4]}-{s14[-4:]}"
        else:
            result["EL_ORIG_LOCATION"] = ""

    # done
    return result

# ---------------- File processing ----------------
def process_file(file_path: Path):
    try:
        logging.info(f"Processing file: {file_path.name}")
        raw = json.loads(file_path.read_text(encoding="utf-8"))
        mapped = map_table8_full(raw)
        output_file = OUTPUT_FOLDER / f"{file_path.stem}_mapped.json"
        output_file.write_text(json.dumps(mapped, indent=4), encoding="utf-8")
        logging.info(f"Mapped output saved: {output_file.name}")
    except Exception:
        logging.exception(f"Error processing file {file_path.name}")

# ---------------- Watcher ----------------
class NewFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        filepath = Path(event.src_path)
        logging.info(f"New file detected: {filepath}")
        if filepath.suffix.lower() == ".json":
            # small delay to allow writer to finish (helps avoid partial reads)
            time.sleep(0.2)
            process_file(filepath)

def watch_folder():
    observer = Observer()
    handler = NewFileHandler()
    observer.schedule(handler, str(WATCH_FOLDER), recursive=False)
    observer.start()
    logging.info(f"Watching folder: {WATCH_FOLDER.resolve()}")

    # Process existing files on startup
    for file_path in sorted(WATCH_FOLDER.glob("*.json")):
        logging.info(f"Processing existing file at startup: {file_path.name}")
        process_file(file_path)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping folder watcher...")
        observer.stop()
    observer.join()

# ---------------- Main ----------------
if __name__ == "__main__":
    logging.info("Table8 mapper starting up...")
    watch_folder()
