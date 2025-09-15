import json
import time
import shutil
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from decimal import Decimal, InvalidOperation
import binascii
import re

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("voice_cdr_mapping")

# Canonical ordered list of 114 EL_* fields (same layout expected)
CANONICAL_FIELDS = [
    "EL_CDR_ID","EL_SRC_CDR_ID","EL_CUST_LOCAL_START_DATE","EL_SESSION_ID","EL_ACTUAL_USAGE","EL_RATE_USAGE","EL_DEBIT_AMOUNT","EL_FREE_UNIT_AMOUNT_OF_DURATION",
    "EL_ACCT_BALANCE_ID1","EL_BALANCE_TYPE1","EL_CUR_BALANCE1","EL_CHG_BALANCE1","EL_RATE_ID1",
    "EL_ACCT_BALANCE_ID2","EL_BALANCE_TYPE2","EL_CUR_BALANCE2","EL_CHG_BALANCE2","EL_RATE_ID2",
    "EL_ACCT_BALANCE_ID3","EL_BALANCE_TYPE3","EL_CUR_BALANCE3","EL_CHG_BALANCE3","EL_RATE_ID3",
    "EL_ACCT_BALANCE_ID4","EL_BALANCE_TYPE4","EL_CUR_BALANCE4","EL_CHG_BALANCE4","EL_RATE_ID4",
    "EL_ACCT_BALANCE_ID5","EL_BALANCE_TYPE5","EL_CUR_BALANCE5","EL_CHG_BALANCE5","EL_RATE_ID5",
    "EL_BUCKET_BALANCE_ID1","EL_BUCKET_BALANCE_TYPE1","EL_BUCKET_CUR_BALANCE1","EL_BUCKET_CHG_BALANCE1","EL_BUCKET_RATE_ID1",
    "EL_BUCKET_BALANCE_ID2","EL_BUCKET_BALANCE_TYPE2","EL_BUCKET_CUR_BALANCE2","EL_BUCKET_CHG_BALANCE2","EL_BUCKET_RATE_ID2",
    "EL_BUCKET_BALANCE_ID3","EL_BUCKET_BALANCE_TYPE3","EL_BUCKET_CUR_BALANCE3","EL_BUCKET_CHG_BALANCE3","EL_BUCKET_RATE_ID3",
    "EL_BUCKET_BALANCE_ID4","EL_BUCKET_BALANCE_TYPE4","EL_BUCKET_CUR_BALANCE4","EL_BUCKET_CHG_BALANCE4","EL_BUCKET_RATE_ID4",
    "EL_BUCKET_BALANCE_ID5","EL_BUCKET_BALANCE_TYPE5","EL_BUCKET_CUR_BALANCE5","EL_BUCKET_CHG_BALANCE5","EL_BUCKET_RATE_ID5",
    "EL_CALLING_PARTY_NUMBER","EL_CALLED_PARTY_NUMBER","EL_CALLING_PARTY_IMSI","EL_CALLED_PARTY_IMSI","EL_SERVICE_FLOW",
    "EL_CALLING_LOCATION_INFO","EL_CALLED_LOCATION_INFO","EL_SEND_RESULT","EL_IMEI","EL_REFUND_INDICATOR",
    "EL_MAIN_OFFERING_ID","EL_CHARGING_PARTY_NUMBER","EL_CHARGE_PARTY_IND","EL_PAY_TYPE","EL_ON_NET_INDICATOR",
    "EL_ROAM_STATE","EL_OPPOSE_NETWORK_TYPE","EL_CALLING_VPN_TOP_GROUP_NUMBER","EL_CALLING_VPN_GROUP_NUMBER","EL_CALLING_VPN_SHORT_NUMBERs",
    "EL_CALLED_VPN_TOP_GROUP_NUMBER","EL_CALLED_VPN_GROUP_NUMBER","EL_CALLED_VPN_SHORT_NUMBER","EL_LAST_EFFECT_OFFERING","EL_ALTERNATE_ID",
    "EL_HOME_ZONE_ID","EL_USER_STATE","EL_PAY_DEFAULT_ACCT_ID","EL_TAX1","EL_TAX2",
    "EL_USER_GROUP_ID","EL_BUSINESS_TYPE","EL_SUBSCRIBER_KEY","EL_ACCOUNT_KEY","EL_DISCOUNT_OF_LAST_EFF_PROD",
    "EL_ADDITIONALBALANCEINFO_CHARGINGSERVICENAME","EL_ADDITIONALBALANCEINFO_USAGETYPE","EL_ADDITIONALBALANCEINFO_USEDAS",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETNAME","EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETUNITTYPE","EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETKINDOFUNIT",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEBEFORE","EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEAFTER","EL_ADDITIONALBALANCEINFO_BUCKETINFO_CARRYOVERBUCKET",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETCOMMITEDUNITS","EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETRESERVEDUNITS","EL_ADDITIONALBALANCEINFO_BUCKETINFO_RATEID",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_PRIMARYCOSTCOMMITTED","EL_ADDITIONALBALANCEINFO_BUCKETINFO_SECONDARYCOSTCOMMITTED","EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXATIONID",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXRATEAPPLIED","EL_ADDITIONALBALANCEINFO_BUCKETINFO_COMMITTEDTAXAMOUNT","EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALTAXAMOUNT",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_TARIFFID","EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALUNITSCHARGED",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALTIMECHARGED","EL_ADDITIONALBALANCEINFO_BUCKETINFO_ROUNDEDTIMECHARGED","EL_ADDITIONALBALANCEINFO_BUCKETINFO_DELTATIME",
    "EL_UNLTD_BUNDLE_NAME","EL_UNLTD_TOTAL_TIME_CHARGED","EL_UNLTD_ROUNDED_UNITS_CHARGED","EL_UNLTD_BUNDLE_UNIT_TYPE","EL_ORIG_LOCATION",
]

# Helpers

def safe_get(node: Any, path: list, default: Any = None) -> Any:
    cur = node
    for p in path:
        if cur is None:
            return default
        if isinstance(p, int):
            if not isinstance(cur, list) or p < 0 or p >= len(cur):
                return default
            cur = cur[p]
        else:
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
    return cur


def to_decimal(v: Any) -> Optional[Decimal]:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        return None


def fmt_decimal_to_float(d: Optional[Decimal]) -> Optional[float]:
    if d is None:
        return None
    try:
        return float(d)
    except Exception:
        try:
            return float(str(d))
        except Exception:
            return None


def last_n_chars(s: Optional[str], n: int) -> str:
    if not s:
        return ""
    s = str(s)
    return s[-n:] if len(s) >= n else s


def decode_location_hex_field(hexstr: Optional[str]) -> str:
    if not hexstr:
        return ""
    s = last_n_chars(hexstr, 18)
    if len(s) < 18:
        s14 = last_n_chars(hexstr, 14)
        if not s14:
            return ""
        return f"{s14[0:6]}-{s14[6:10]}-{s14[10:14]}"
    tac_hex = s[0:4]
    mccmnc_hex = s[4:10]
    eci_hex = s[10:18]
    try:
        tac_dec = str(int(tac_hex, 16))
    except Exception:
        tac_dec = tac_hex
    pairs = [mccmnc_hex[i:i+2] for i in range(0, len(mccmnc_hex), 2)]
    swapped = "".join(p[::-1] for p in pairs)
    swapped_clean = swapped.replace('F', '').replace('f', '')
    try:
        eci_int = int(eci_hex, 16)
    except Exception:
        eci_int = 0
    enb = str(eci_int % 256)
    cell = str(eci_int // 256)
    return f"{swapped_clean}-{tac_dec}-{enb}-{cell}"


def imei_from_user_equipment_value(hex_or_str: Optional[str]) -> str:
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
    even_chars = "".join(ch for i, ch in enumerate(src) if ((i + 1) % 2) == 0)
    digits = re.sub(r'\D', '', even_chars)
    if len(digits) >= 16:
        return digits[:16]
    return digits.ljust(16, "0")


def enforce_canonical(mapped: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    # numeric fields explicit
    numeric_fields = {"EL_DEBIT_AMOUNT","EL_ON_NET_INDICATOR","EL_TAX1","EL_TAX2","EL_UNLTD_ROUNDED_UNITS_CHARGED","EL_UNLTD_TOTAL_TIME_CHARGED"}
    for i in range(1,6):
        numeric_fields.add(f"EL_CUR_BALANCE{i}")
        numeric_fields.add(f"EL_CHG_BALANCE{i}")
     
    for key in CANONICAL_FIELDS:
        if key in mapped and mapped[key] is not None and mapped[key] != "":
            out[key] = mapped[key]
        else:
            out[key] = 0.0 if key in numeric_fields else ""
    return out


def validate_canonical_record(rec: Dict[str, Any]) -> Tuple[bool, str]:
    # ensure keys count and order
    if list(rec.keys()) != CANONICAL_FIELDS:
        return False, f"keys mismatch: expected {len(CANONICAL_FIELDS)} fields, got {len(rec.keys())}"
    # numeric check: allow ints/floats or comma-separated numeric strings for bucket lists
    numeric_fields = {"EL_DEBIT_AMOUNT","EL_ON_NET_INDICATOR","EL_TAX1","EL_TAX2","EL_UNLTD_ROUNDED_UNITS_CHARGED","EL_UNLTD_TOTAL_TIME_CHARGED"}
    for i in range(1,6):
        numeric_fields.update({f"EL_CUR_BALANCE{i}", f"EL_CHG_BALANCE{i}", f"EL_BUCKET_CUR_BALANCE{i}", f"EL_BUCKET_CHG_BALANCE{i}"})

    def is_numeric_value(v: Any) -> bool:
        if isinstance(v, (int, float)):
            return True
        if isinstance(v, str):
            parts = [p.strip() for p in v.split(",") if p.strip() != ""]
            if not parts:
                return False
            for p in parts:
                try:
                    float(p)
                except Exception:
                    return False
            return True
        return False

    for k in numeric_fields:
        v = rec.get(k)
        if not is_numeric_value(v):
            return False, f"field {k} not numeric or numeric-list: {v}"
    return True, ""


def extract_imsi_from_extensions(extensions: list, event_label: Any) -> Tuple[str,str]:
    # returns (calling_imsi, called_imsi)
    calling = ""
    called = ""
    sub_ext = None
    for ext in extensions:
        if safe_get(ext, ["recordProperty"]) == "listOfSubscriptionID":
            sub_ext = ext
            break
    subs = []
    if sub_ext:
        for sid in safe_get(sub_ext, ["recordSubExtensions"], []) or []:
            if safe_get(sid, ["recordProperty"]) == "subscriptionId":
                elems = safe_get(sid, ["recordElements"], {}) or {}
                dtype = elems.get("subscriptionIDType") or elems.get("subscriptionIdType")
                ddata = elems.get("subscriptionIDData") or elems.get("subscriptionIdData")
                if ddata is None:
                    continue
                ds = str(ddata)
                if ds.lower().startswith("imsi-"):
                    ds = ds.split('-',1)[1]
                subs.append({"type": str(dtype) if dtype is not None else "", "data": ds})
    try:
        # calling IMSI when event label is 1 or 821
        if str(event_label) in ("1", "821"):
            for s in subs:
                if s["type"] == "1":
                    calling = s["data"]
                    break
        # called IMSI when event label is 2
        if str(event_label) == "2":
            for s in subs:
                if s["type"] == "1":
                    called = s["data"]
                    break
    except Exception:
        pass
    return calling, called


def extract_subid_type0(extensions: list) -> Optional[str]:
    """Return first subscriptionId.subscriptionIDData where subscriptionIDType == '0'"""
    sub_ext = None
    for ext in extensions:
        if safe_get(ext, ["recordProperty"]) == "listOfSubscriptionID":
            sub_ext = ext
            break
    if not sub_ext:
        return None
    for sid in safe_get(sub_ext, ["recordSubExtensions"], []) or []:
        if safe_get(sid, ["recordProperty"]) == "subscriptionId":
            elems = safe_get(sid, ["recordElements"], {}) or {}
            dtype = elems.get("subscriptionIDType") or elems.get("subscriptionIdType")
            ddata = elems.get("subscriptionIDData") or elems.get("subscriptionIdData")
            if ddata is None:
                continue
            if str(dtype) == "0":
                return str(ddata)
    return None


def extract_any_subid(extensions: list) -> Optional[str]:
    """Return first subscriptionId.subscriptionIDData regardless of type."""
    sub_ext = None
    for ext in extensions:
        if safe_get(ext, ["recordProperty"]) == "listOfSubscriptionID":
            sub_ext = ext
            break
    if not sub_ext:
        return None
    for sid in safe_get(sub_ext, ["recordSubExtensions"], []) or []:
        if safe_get(sid, ["recordProperty"]) == "subscriptionId":
            elems = safe_get(sid, ["recordElements"], {}) or {}
            ddata = elems.get("subscriptionIDData") or elems.get("subscriptionIdData")
            if ddata is None:
                continue
            return str(ddata)
    return None


def map_voice(cdr_json: Dict[str, Any], imei_normalize: bool = True) -> Dict[str, Any]:
    generic = safe_get(cdr_json, ["original","payload","genericRecord"], {}) or cdr_json
    record_elems = safe_get(generic, ["recordElements"], {}) or {}
    extensions = safe_get(generic, ["recordExtensions"], []) or []
    cbl = cdr_json.get("CBL_TAG") or {}

    out = {}
    # Phase1
    out["EL_CDR_ID"] = record_elems.get("sessionId") or record_elems.get("recordId") or ""
    out["EL_SRC_CDR_ID"] = record_elems.get("sessionSequenceNumber") or ""
    # EL_CUST_LOCAL_START_DATE: prefer callAnswerTime > recordOpeningTime > generationTimestamp
    out["EL_CUST_LOCAL_START_DATE"] = record_elems.get("callAnswerTime") or record_elems.get("recordOpeningTime") or record_elems.get("generationTimestamp") or ""
    # EL_SESSION_ID same as sessionId
    out["EL_SESSION_ID"] = record_elems.get("sessionId") or ""

    # EL_ACTUAL_USAGE and EL_RATE_USAGE: take first totalTimeConsumed from listOfMscc.mscc if present
    el_actual_usage = None
    el_rate_usage = None
    list_of_mscc_ext = None
    for ext in extensions:
        if safe_get(ext, ["recordProperty"]) == "listOfMscc":
            list_of_mscc_ext = ext
            break
    if list_of_mscc_ext:
        for mscc in safe_get(list_of_mscc_ext, ["recordSubExtensions"], []) or []:
            if safe_get(mscc, ["recordProperty"]) == "mscc":
                elems = safe_get(mscc, ["recordElements"], {}) or {}
                tv = elems.get("totalTimeConsumed")
                if tv is not None:
                    try:
                        d = to_decimal(tv)
                        if d is not None:
                            el_actual_usage = fmt_decimal_to_float(d)
                            el_rate_usage = el_actual_usage
                            break
                    except Exception:
                        pass
    out["EL_ACTUAL_USAGE"] = el_actual_usage if el_actual_usage is not None else 0.0
    out["EL_RATE_USAGE"] = el_rate_usage if el_rate_usage is not None else 0.0

    # EL_DEBIT_AMOUNT and EL_FREE_UNIT_AMOUNT_OF_DURATION: loop subscriptions per spec
    el_debit_amt = None
    el_free_units = None
    # iterate mscc blocks and look into deviceInfo->subscriptionInfo->chargingServiceInfo
    if list_of_mscc_ext:
        for mscc in safe_get(list_of_mscc_ext, ["recordSubExtensions"], []) or []:
            if safe_get(mscc, ["recordProperty"]) != "mscc":
                continue
            for sub in safe_get(mscc, ["recordSubExtensions"], []) or []:
                if safe_get(sub, ["recordProperty"]) == "deviceInfo":
                    for s2 in safe_get(sub, ["recordSubExtensions"], []) or []:
                        if safe_get(s2, ["recordProperty"]) == "subscriptionInfo":
                            for s3 in safe_get(s2, ["recordSubExtensions"], []) or []:
                                if safe_get(s3, ["recordProperty"]) == "chargingServiceInfo":
                                    # detect bucketInfo existence
                                    has_bucket = False
                                    for s4 in safe_get(s3, ["recordSubExtensions"], []) or []:
                                        if safe_get(s4, ["recordProperty"]) == "bucketInfo":
                                            has_bucket = True
                                            break
                                    # find accountInfo and noCharge
                                    account_info = {}
                                    nocharge_val = None
                                    for s4 in safe_get(s3, ["recordSubExtensions"], []) or []:
                                        if safe_get(s4, ["recordProperty"]) == "accountInfo":
                                            account_info = safe_get(s4, ["recordElements"], {}) or {}
                                        if safe_get(s4, ["recordProperty"]) == "noCharge":
                                            nocharge_elems = safe_get(s4, ["recordElements"], {}) or {}
                                            nocharge_val = nocharge_elems.get("noChargeCommittedUnits")
                                    # apply rules
                                    if (not has_bucket) and account_info:
                                        # prefer accountBalanceCommittedBR then accountBalanceCommitted
                                        v = account_info.get("accountBalanceCommittedBR")
                                        if v is None:
                                            v = account_info.get("accountBalanceCommitted")
                                        if v is not None and el_debit_amt is None:
                                            dv = to_decimal(v)
                                            if dv is not None:
                                                el_debit_amt = fmt_decimal_to_float(dv)
                                        if nocharge_val is not None and el_free_units is None:
                                            el_free_units = to_decimal(nocharge_val)
                                            if el_free_units is not None:
                                                el_free_units = fmt_decimal_to_float(el_free_units)
                                    else:
                                        # fallback: use accountBalanceBefore - accountBalanceAfter for first occurrence
                                        if account_info and el_debit_amt is None:
                                            bef = to_decimal(account_info.get("accountBalanceBefore"))
                                            aft = to_decimal(account_info.get("accountBalanceAfter"))
                                            if bef is not None and aft is not None:
                                                diff = bef - aft
                                                el_debit_amt = fmt_decimal_to_float(diff)
            if el_debit_amt is not None and el_free_units is not None:
                break
    out["EL_DEBIT_AMOUNT"] = el_debit_amt if el_debit_amt is not None else 0.0
    out["EL_FREE_UNIT_AMOUNT_OF_DURATION"] = el_free_units if el_free_units is not None else ""

    # Phase3 common
    # derive event_label from possible locations
    event_label = None
    if isinstance(cbl, dict) and cbl.get("EL_EVENT_LABEL_VAL") is not None:
        event_label = cbl.get("EL_EVENT_LABEL_VAL")
    if event_label is None:
        event_label = record_elems.get("EL_EVENT_LABEL_VAL") or record_elems.get("eventLabel") or record_elems.get("eventLabelValue")

    # Determine mscc subRecordEventType early for conditional rules
    el_service_flow = None
    if list_of_mscc_ext:
        for mscc in safe_get(list_of_mscc_ext, ["recordSubExtensions"], []) or []:
            if safe_get(mscc, ["recordProperty"]) == "mscc":
                first_mscc_elems = safe_get(mscc, ["recordElements"], {}) or {}
                el_service_flow = first_mscc_elems.get("subRecordEventType") or el_service_flow
                break
    out["EL_SERVICE_FLOW"] = el_service_flow or (record_elems.get("subRecordEventType") or "VOICE")

    # Determine subRecordEventType and roaming for later rules
    subrec_type = el_service_flow or record_elems.get("subRecordEventType") or ""
    roaming_flag = (str(record_elems.get("roamingIndicator") or record_elems.get("RoamingStatus") or "")).upper()

    # IMSI extraction
    calling_imsi, called_imsi = extract_imsi_from_extensions(extensions, event_label)

    # prefer subscriptionId type '0' when present
    subid0 = extract_subid_type0(extensions)
    any_subid = extract_any_subid(extensions)

    # conditional rule: if (roaming == 'ROAMING' and subRecordEventType == 'MTC') OR subRecordEventType == 'FWD'
    cond_use_subid = (roaming_flag == "ROAMING" and subrec_type == "MTC") or (subrec_type == "FWD")

    calling_number = ""
    if cond_use_subid and subid0:
        calling_number = str(subid0)
    elif cond_use_subid and any_subid:
        calling_number = str(any_subid)
    else:
        # fallback: prefer subid0 then any_subid, else callingPartyAddress
        if subid0:
            calling_number = str(subid0)
        elif any_subid:
            calling_number = str(any_subid)
        else:
            calling_number = record_elems.get("callingPartyAddress") or record_elems.get("originatorAddress") or record_elems.get("callingParty") or ""

    # normalization: if does not start with 251 and length < 10, remove leading 0 and prepend 251
    if calling_number and not calling_number.startswith("251") and len(calling_number) < 10:
        if calling_number.startswith("0"):
            calling_number = calling_number.lstrip("0")
        calling_number = "251" + calling_number

    out["EL_CALLING_PARTY_NUMBER"] = calling_number
    called_number = record_elems.get("calledPartyAddress") or record_elems.get("recipientAddress") or record_elems.get("calledParty") or ""
    # ensure called number normalization if present and short
    if called_number and not called_number.startswith("251") and len(called_number) < 10:
        if called_number.startswith("0"):
            called_number = called_number.lstrip("0")
            called_number = "251" + called_number
    out["EL_CALLED_PARTY_NUMBER"] = called_number
    # IMSI
    out["EL_CALLING_PARTY_IMSI"] = calling_imsi
    out["EL_CALLED_PARTY_IMSI"] = called_imsi
    # Determine mscc subRecordEventType and use for service flow
    el_service_flow = None
    if list_of_mscc_ext:
        for mscc in safe_get(list_of_mscc_ext, ["recordSubExtensions"], []) or []:
            if safe_get(mscc, ["recordProperty"]) == "mscc":
                first_mscc_elems = safe_get(mscc, ["recordElements"], {}) or {}
                el_service_flow = first_mscc_elems.get("subRecordEventType") or el_service_flow
                break
    out["EL_SERVICE_FLOW"] = el_service_flow or (record_elems.get("subRecordEventType") or "VOICE")

    # location
    user_loc = record_elems.get("userLocationInformation") or record_elems.get("origUserLocationInfo") or ""
    rat = record_elems.get("rATType") or record_elems.get("ratType") or record_elems.get("rAT")
    if user_loc:
        if str(rat) == "6":
            out["EL_CALLING_LOCATION_INFO"] = decode_location_hex_field(user_loc)
            out["EL_CALLED_LOCATION_INFO"] = out["EL_CALLING_LOCATION_INFO"]
        else:
            tail = last_n_chars(user_loc, 13)
            if len(tail) >= 13:
                a = tail[0:5]; b = tail[5:9]; c = tail[9:13]
                out["EL_CALLING_LOCATION_INFO"] = f"{a}-{b}-{c}"
                out["EL_CALLED_LOCATION_INFO"] = out["EL_CALLING_LOCATION_INFO"]
            else:
                out["EL_CALLING_LOCATION_INFO"] = tail
                out["EL_CALLED_LOCATION_INFO"] = tail
    else:
        out["EL_CALLING_LOCATION_INFO"] = ""
        out["EL_CALLED_LOCATION_INFO"] = ""

    out["EL_SEND_RESULT"] = record_elems.get("resultCode") or ""
    # EL_IMEI: apply optional normalization
    raw_imei = record_elems.get("userEquipmentValue") or record_elems.get("imei") or ""
    if imei_normalize:
        out["EL_IMEI"] = imei_from_user_equipment_value(raw_imei)
    else:
        out["EL_IMEI"] = raw_imei

    out["EL_REFUND_INDICATOR"] = ""
    out["EL_MAIN_OFFERING_ID"] = ""
    # Charging party follows same normalization rules as calling
    out["EL_CHARGING_PARTY_NUMBER"] = out["EL_CALLING_PARTY_NUMBER"]
    out["EL_CHARGE_PARTY_IND"] = ""
    out["EL_PAY_TYPE"] = (record_elems.get("EL_PRE_POST") or record_elems.get("prePost") or (cbl.get("EL_PRE_POST") if isinstance(cbl, dict) else ""))
    on_net = record_elems.get("isOnNet")
    if isinstance(on_net, bool):
        out["EL_ON_NET_INDICATOR"] = 1 if on_net else 0
    elif isinstance(on_net, str):
        out["EL_ON_NET_INDICATOR"] = 1 if on_net.lower() == "true" else 0
    else:
        # default to 0 when unknown to ensure numeric
        out["EL_ON_NET_INDICATOR"] = 0
    out["EL_ROAM_STATE"] = record_elems.get("RoamingStatus") or record_elems.get("roamingIndicator") or ""
    out["EL_OPPOSE_NETWORK_TYPE"] = record_elems.get("rATType") or record_elems.get("ratType") or ""
    out["EL_CALLING_VPN_TOP_GROUP_NUMBER"] = record_elems.get("groupID") or ""
    out["EL_CALLING_VPN_GROUP_NUMBER"] = ""
    out["EL_CALLING_VPN_SHORT_NUMBERs"] = ""
    out["EL_CALLED_VPN_TOP_GROUP_NUMBER"] = ""
    out["EL_CALLED_VPN_GROUP_NUMBER"] = ""
    out["EL_CALLED_VPN_SHORT_NUMBER"] = ""
    out["EL_LAST_EFFECT_OFFERING"] = ""
    # alternate ids
    alt_ids = []
    for mscc in extensions:
        if safe_get(mscc, ["recordProperty"]) == "listOfMscc":
            for sub in safe_get(mscc, ["recordSubExtensions"], []) or []:
                for d in safe_get(sub, ["recordSubExtensions"], []) or []:
                    # dive into deviceInfo->subscriptionInfo
                    if safe_get(d, ["recordProperty"]) == "deviceInfo":
                        for s2 in safe_get(d, ["recordSubExtensions"], []) or []:
                            if safe_get(s2, ["recordProperty"]) == "subscriptionInfo":
                                se = safe_get(s2, ["recordElements"], {}) or {}
                                a = se.get("alternateId")
                                if a:
                                    alt_ids.append(str(a))
    out["EL_ALTERNATE_ID"] = "~".join(alt_ids) if alt_ids else ""
    out["EL_HOME_ZONE_ID"] = ""
    out["EL_USER_STATE"] = record_elems.get("deviceState") or ""
    out["EL_PAY_DEFAULT_ACCT_ID"] = ""

    # EL_MAIN_OFFERING_ID: first subscriptionInfo.bundleName where no bucketInfo
    main_off = ""
    if list_of_mscc_ext:
        for mscc in safe_get(list_of_mscc_ext, ["recordSubExtensions"], []) or []:
            if safe_get(mscc, ["recordProperty"]) != "mscc":
                continue
            for sub in safe_get(mscc, ["recordSubExtensions"], []) or []:
                if safe_get(sub, ["recordProperty"]) == "deviceInfo":
                    for s2 in safe_get(sub, ["recordSubExtensions"], []) or []:
                        if safe_get(s2, ["recordProperty"]) == "subscriptionInfo":
                            bundle = safe_get(s2, ["recordElements","bundleName"]) or safe_get(s2, ["recordElements","bundle_name"]) or ""
                            has_bucket = False
                            for s3 in safe_get(s2, ["recordSubExtensions"], []) or []:
                                if safe_get(s3, ["recordProperty"]) == "chargingServiceInfo":
                                    for s4 in safe_get(s3, ["recordSubExtensions"], []) or []:
                                        if safe_get(s4, ["recordProperty"]) == "bucketInfo":
                                            has_bucket = True
                                            break
                            if bundle and not has_bucket:
                                main_off = bundle
                                break
                    if main_off:
                        break
            if main_off:
                break
    out["EL_MAIN_OFFERING_ID"] = main_off

    # taxes: EL_TAX1 from first account_only committedTaxAmount, EL_TAX2 from first bucket committedTaxAmount
    el_tax1 = None
    el_tax2 = None
    # try account_only and bucket_subs created later; as fallback use CBL
    if 'account_only' in locals() and account_only:
        ca = account_only[0]
        el_tax1 = to_decimal(ca.get("committedTaxAmount"))
    if 'bucket_subs' in locals() and bucket_subs:
        first_buckets = bucket_subs[0].get('buckets', [])
        if first_buckets:
            el_tax2 = first_buckets[0].get('committedTaxAmount') if isinstance(first_buckets[0].get('committedTaxAmount'), Decimal) else to_decimal(first_buckets[0].get('committedTaxAmount'))
    if el_tax1 is None and isinstance(cbl, dict):
        el_tax1 = to_decimal(cbl.get('EL_TAX1'))
    if el_tax2 is None and isinstance(cbl, dict):
        el_tax2 = to_decimal(cbl.get('EL_TAX2'))
    out["EL_TAX1"] = fmt_decimal_to_float(el_tax1) if el_tax1 is not None else 0.0
    out["EL_TAX2"] = fmt_decimal_to_float(el_tax2) if el_tax2 is not None else 0.0

    # user group id from groupID
    out["EL_USER_GROUP_ID"] = record_elems.get("groupID") or ""

    # Phase2: extract account-only subscriptionInfo blocks and bucket-containing subscriptionInfo blocks
    account_only = []
    bucket_subs = []
    if list_of_mscc_ext:
        for mscc in safe_get(list_of_mscc_ext, ["recordSubExtensions"], []) or []:
            if safe_get(mscc, ["recordProperty"]) != "mscc":
                continue
            for sub in safe_get(mscc, ["recordSubExtensions"], []) or []:
                if safe_get(sub, ["recordProperty"]) == "deviceInfo":
                    for s2 in safe_get(sub, ["recordSubExtensions"], []) or []:
                        if safe_get(s2, ["recordProperty"]) == "subscriptionInfo":
                            bundle = safe_get(s2, ["recordElements", "bundleName"]) or safe_get(s2, ["recordElements", "bundle_name"]) or ""
                            has_bucket = False
                            acct_info = None
                            buckets = []
                            for s3 in safe_get(s2, ["recordSubExtensions"], []) or []:
                                if safe_get(s3, ["recordProperty"]) == "chargingServiceInfo":
                                    for s4 in safe_get(s3, ["recordSubExtensions"], []) or []:
                                        if safe_get(s4, ["recordProperty"]) == "bucketInfo":
                                            has_bucket = True
                                            be = safe_get(s4, ["recordElements"], {}) or {}
                                            buckets.append({
                                                "bucketName": be.get("bucketName") or "",
                                                "bucketUnitType": be.get("bucketUnitType") or "",
                                                "bucketBalanceBefore": to_decimal(be.get("bucketBalanceBefore")),
                                                "bucketBalanceAfter": to_decimal(be.get("bucketBalanceAfter")),
                                                "bucketCommitedUnits": to_decimal(be.get("bucketCommitedUnits") or be.get("bucketCommittedUnits")),
                                                "rateId": be.get("rateId") or ""
                                            })
                                        if safe_get(s4, ["recordProperty"]) == "accountInfo" and acct_info is None:
                                            acct_info = safe_get(s4, ["recordElements"], {}) or {}
                            if has_bucket and buckets:
                                bucket_subs.append({"bundleName": bundle, "buckets": buckets})
                            elif acct_info:
                                account_only.append({"bundleName": bundle, "acct": acct_info})

    # populate first 5 account-only slots
    for idx in range(5):
        n = idx + 1
        if idx < len(account_only):
            acct_entry = account_only[idx]
            acct = acct_entry.get('acct')
            out[f"EL_ACCT_BALANCE_ID{n}"] = acct.get("accountID") or ""
            out[f"EL_BALANCE_TYPE{n}"] = acct.get("accountType") or ""
            cur = to_decimal(acct.get("accountBalanceAfter"))
            out[f"EL_CUR_BALANCE{n}"] = fmt_decimal_to_float(cur) if cur is not None else 0.0
            before = to_decimal(acct.get("accountBalanceBefore"))
            if before is not None and cur is not None:
                diff = before - cur
                if diff >= Decimal(0):
                    out[f"EL_CHG_BALANCE{n}"] = fmt_decimal_to_float(diff) if fmt_decimal_to_float(diff) is not None else 0.0
                else:
                    committed = to_decimal(acct.get("accountBalanceCommitted")) or to_decimal(acct.get("accountBalanceCommittedBR")) or Decimal(0)
                    secondary = to_decimal(acct.get("secondaryCostCommitted")) or Decimal(0)
                    ssum = committed + secondary
                    out[f"EL_CHG_BALANCE{n}"] = fmt_decimal_to_float(ssum) if fmt_decimal_to_float(ssum) is not None else 0.0
            else:
                out[f"EL_CHG_BALANCE{n}"] = 0.0
            out[f"EL_RATE_ID{n}"] = acct.get("rateId") or ""
        else:
            out[f"EL_ACCT_BALANCE_ID{n}"] = ""
            out[f"EL_BALANCE_TYPE{n}"] = ""
            out[f"EL_CUR_BALANCE{n}"] = 0.0
            out[f"EL_CHG_BALANCE{n}"] = 0.0
            out[f"EL_RATE_ID{n}"] = ""

    # populate first 5 bucket slots (concatenate values per subscription block)
    for idx in range(5):
        n = idx + 1
        if idx < len(bucket_subs):
            entry = bucket_subs[idx]
            bundle = entry.get("bundleName") or ""
            buckets = entry.get("buckets") or []
            names = [b.get("bucketName") for b in buckets if b.get("bucketName")]
            id_val = (bundle + "-" + ",".join(names)) if bundle and names else (bundle or ",".join(names))
            out[f"EL_BUCKET_BALANCE_ID{n}"] = id_val
            out[f"EL_BUCKET_BALANCE_TYPE{n}"] = ",".join([b.get("bucketUnitType") for b in buckets if b.get("bucketUnitType")])
            # cur balances as comma-separated floats
            cur_vals = []
            for b in buckets:
                ba = b.get("bucketBalanceAfter")
                cur_vals.append(str(fmt_decimal_to_float(ba)) if ba is not None else "0.0")
            out[f"EL_BUCKET_CUR_BALANCE{n}"] = ",".join(cur_vals)
            # change values per bucket
            chg_vals = []
            for b in buckets:
                before = b.get("bucketBalanceBefore")
                after = b.get("bucketBalanceAfter")
                if before is not None and after is not None:
                    diff = before - after
                    if diff >= Decimal(0):
                        chg_vals.append(str(fmt_decimal_to_float(diff) if fmt_decimal_to_float(diff) is not None else 0.0))
                    else:
                        committed = b.get("bucketCommitedUnits") or Decimal(0)
                        chg_vals.append(str(fmt_decimal_to_float(committed) if fmt_decimal_to_float(committed) is not None else 0.0))
                else:
                    # fallback to committed units or 0
                    committed = b.get("bucketCommitedUnits") or Decimal(0)
                    chg_vals.append(str(fmt_decimal_to_float(committed) if fmt_decimal_to_float(committed) is not None else 0.0))
            out[f"EL_BUCKET_CHG_BALANCE{n}"] = ",".join(chg_vals)
            out[f"EL_BUCKET_RATE_ID{n}"] = ",".join([b.get("rateId") for b in buckets if b.get("rateId")])
        else:
            out[f"EL_BUCKET_BALANCE_ID{n}"] = ""
            out[f"EL_BUCKET_BALANCE_TYPE{n}"] = ""
            out[f"EL_BUCKET_CUR_BALANCE{n}"] = 0.0
            out[f"EL_BUCKET_CHG_BALANCE{n}"] = 0.0
            out[f"EL_BUCKET_RATE_ID{n}"] = ""

    # aggregated additionalBalanceInfo fields empty by default
    for k in CANONICAL_FIELDS:
        if k.startswith("EL_ADDITIONALBALANCEINFO_"):
            out[k] = ""

    # Phase4: extract additionalBalanceInfo across all subscriptionInfo blocks and aggregate
    additional_blocks = []
    if list_of_mscc_ext:
        for mscc in safe_get(list_of_mscc_ext, ["recordSubExtensions"], []) or []:
            if safe_get(mscc, ["recordProperty"]) != "mscc":
                continue
            for sub in safe_get(mscc, ["recordSubExtensions"], []) or []:
                if safe_get(sub, ["recordProperty"]) == "deviceInfo":
                    for s2 in safe_get(sub, ["recordSubExtensions"], []) or []:
                        if safe_get(s2, ["recordProperty"]) == "subscriptionInfo":
                            for s3 in safe_get(s2, ["recordSubExtensions"], []) or []:
                                if safe_get(s3, ["recordProperty"]) == "chargingServiceInfo":
                                    for s4 in safe_get(s3, ["recordSubExtensions"], []) or []:
                                        if safe_get(s4, ["recordProperty"]) == "additionalBalanceInfo":
                                            ab_elems = safe_get(s4, ["recordElements"], {}) or {}
                                            chargingName = ab_elems.get("chargingServiceName") or ab_elems.get("chargingservicename") or ""
                                            # find adjustBalanceInfo
                                            adj = None
                                            for s5 in safe_get(s4, ["recordSubExtensions"], []) or []:
                                                if safe_get(s5, ["recordProperty"]) == "adjustBalanceInfo":
                                                    adj = s5
                                                    break
                                            adj_elems = safe_get(adj, ["recordElements"], {}) or {}
                                            # find bucketInfo under adj or additionalBalanceInfo
                                            bucket_elems = {}
                                            if adj:
                                                for b in safe_get(adj, ["recordSubExtensions"], []) or []:
                                                    if safe_get(b, ["recordProperty"]) == "bucketInfo":
                                                        bucket_elems = safe_get(b, ["recordElements"], {}) or {}
                                                        break
                                            if not bucket_elems:
                                                for b in safe_get(s4, ["recordSubExtensions"], []) or []:
                                                    if safe_get(b, ["recordProperty"]) == "bucketInfo":
                                                        bucket_elems = safe_get(b, ["recordElements"], {}) or {}
                                                        break
                                            additional_blocks.append({
                                                "chargingServiceName": chargingName,
                                                "usageType": adj_elems.get("usageType") or "",
                                                "usedAs": adj_elems.get("usedAs") or "",
                                                "bucketName": bucket_elems.get("bucketName") or "",
                                                "bucketUnitType": bucket_elems.get("bucketUnitType") or "",
                                                "bucketKindOfUnit": bucket_elems.get("bucketKindOfUnit") or "",
                                                "bucketBalanceBefore": bucket_elems.get("bucketBalanceBefore"),
                                                "bucketBalanceAfter": bucket_elems.get("bucketBalanceAfter"),
                                                "carryOverBucket": bucket_elems.get("carryOverBucket") or "",
                                                "bucketCommitedUnits": bucket_elems.get("bucketCommitedUnits") or bucket_elems.get("bucketCommittedUnits"),
                                                "bucketReservedUnits": bucket_elems.get("bucketReservedUnits"),
                                                "rateId": bucket_elems.get("rateId") or "",
                                                "primaryCostCommitted": bucket_elems.get("primaryCostCommitted"),
                                                "secondaryCostCommitted": bucket_elems.get("secondaryCostCommitted"),
                                                "taxationID": bucket_elems.get("taxationID") or bucket_elems.get("taxationId") or "",
                                                "taxRateApplied": bucket_elems.get("taxRateApplied"),
                                                "committedTaxAmount": bucket_elems.get("committedTaxAmount"),
                                                "totalTaxAmount": bucket_elems.get("totalTaxAmount"),
                                                "tariffID": bucket_elems.get("tariffID") or bucket_elems.get("tariffId") or "",
                                                "totalUnitsCharged": bucket_elems.get("totalUnitsCharged"),
                                                "totalTimeCharged": bucket_elems.get("totalTimeCharged"),
                                                "roundedTimeCharged": bucket_elems.get("roundedTimeCharged"),
                                                "deltaTime": bucket_elems.get("deltaTime"),
                                            })
    # helper to join values preserving order
    def _join_vals(key):
        parts = []
        for a in additional_blocks:
            v = a.get(key)
            if v is None or v == "":
                continue
            parts.append(str(v))
        return ",".join(parts)

    out["EL_ADDITIONALBALANCEINFO_CHARGINGSERVICENAME"] = _join_vals("chargingServiceName")
    out["EL_ADDITIONALBALANCEINFO_USAGETYPE"] = _join_vals("usageType")
    out["EL_ADDITIONALBALANCEINFO_USEDAS"] = _join_vals("usedAs")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETNAME"] = _join_vals("bucketName")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETUNITTYPE"] = _join_vals("bucketUnitType")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETKINDOFUNIT"] = _join_vals("bucketKindOfUnit")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEBEFORE"] = _join_vals("bucketBalanceBefore")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEAFTER"] = _join_vals("bucketBalanceAfter")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_CARRYOVERBUCKET"] = _join_vals("carryOverBucket")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETCOMMITEDUNITS"] = _join_vals("bucketCommitedUnits")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETRESERVEDUNITS"] = _join_vals("bucketReservedUnits")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_RATEID"] = _join_vals("rateId")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_PRIMARYCOSTCOMMITTED"] = _join_vals("primaryCostCommitted")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_SECONDARYCOSTCOMMITTED"] = _join_vals("secondaryCostCommitted")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXATIONID"] = _join_vals("taxationID")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXRATEAPPLIED"] = _join_vals("taxRateApplied")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_COMMITTEDTAXAMOUNT"] = _join_vals("committedTaxAmount")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALTAXAMOUNT"] = _join_vals("totalTaxAmount")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TARIFFID"] = _join_vals("tariffID")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALUNITSCHARGED"] = _join_vals("totalUnitsCharged")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALTIMECHARGED"] = _join_vals("totalTimeCharged")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_ROUNDEDTIMECHARGED"] = _join_vals("roundedTimeCharged")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_DELTATIME"] = _join_vals("deltaTime")

    # Phase5 defaults
    # Unlimited bundle detection
    out["EL_UNLTD_BUNDLE_NAME"] = ""
    out["EL_UNLTD_TOTAL_TIME_CHARGED"] = 0.0
    out["EL_UNLTD_ROUNDED_UNITS_CHARGED"] = ""
    out["EL_UNLTD_BUNDLE_UNIT_TYPE"] = ""
    if account_only:
        for entry in account_only:
            acct = entry.get('acct') or {}
            bundle_name = entry.get('bundleName') or ""
            committed = to_decimal(acct.get('accountBalanceCommitted')) or to_decimal(acct.get('accountBalanceCommittedBR'))
            total_time = to_decimal(acct.get('totalTimeCharged')) or to_decimal(acct.get('roundedTimeCharged'))
            if committed is not None and committed == Decimal(0) and total_time is not None and total_time > Decimal(0):
                out["EL_UNLTD_BUNDLE_NAME"] = bundle_name
                out["EL_UNLTD_TOTAL_TIME_CHARGED"] = fmt_decimal_to_float(total_time) if fmt_decimal_to_float(total_time) is not None else 0.0
                out["EL_UNLTD_BUNDLE_UNIT_TYPE"] = "TIME"
                # leave EL_UNLTD_ROUNDED_UNITS_CHARGED empty (not applicable for time)
                break

    # finally enforce canonical and return
    return enforce_canonical(out)


def read_json_stable(path: Path, retries: int = 5, delay: float = 0.2) -> dict:
    for i in range(retries):
        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            time.sleep(delay)
    raise


def process_input_file(path: Path, out_dir: Path, processed_dir: Path, imei_normalize: bool = True) -> None:
    logger.info(f"Processing file: {path.name}")
    try:
        data = read_json_stable(path)
    except Exception as e:
        logger.error(f"Failed to read {path}: {e}")
        return
    # support list or single
    records = []
    if isinstance(data, dict) and "records" in data and isinstance(data["records"], dict):
        records = list(data["records"].values())
    elif isinstance(data, list):
        records = data
    else:
        records = [data]

    mapped = []
    rejects = []
    for rec in records:
        try:
            m = map_voice(rec, imei_normalize=imei_normalize)
            valid, msg = validate_canonical_record(m)
            if valid:
                mapped.append(m)
            else:
                rejects.append({"reason": msg, "record": m})
        except Exception as e:
            logger.exception("Mapping failed")
            rejects.append({"reason": str(e), "record": rec})

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{path.stem}_voice_phase1.json"
    tmp = out_path.with_suffix('.json.tmp')
    try:
        with tmp.open('w', encoding='utf-8') as f:
            json.dump(mapped, f, indent=2, ensure_ascii=False)
        tmp.replace(out_path)
        logger.info(f"Wrote {out_path}")
        if rejects:
            rej_dir = out_dir / 'rejects'
            rej_dir.mkdir(parents=True, exist_ok=True)
            rej_path = rej_dir / f"{path.stem}_rejects.json"
            with rej_path.open('w', encoding='utf-8') as rf:
                json.dump(rejects, rf, indent=2, ensure_ascii=False)
        processed_dir.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(path), str(processed_dir / path.name))
        except Exception as mv_e:
            logger.error(f"Failed to move file: {mv_e}")
    except Exception as e:
        logger.exception(f"Failed to write output: {e}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--in', dest='in_dir', default='in')
    parser.add_argument('--out', dest='out_dir', default='out')
    parser.add_argument('--processed', dest='processed_dir', default='processed')
    parser.add_argument('--no-imei-normalize', dest='imei_normalize', action='store_false')
    parser.add_argument('--watch', dest='watch', action='store_true')
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    processed_dir = Path(args.processed_dir)
    imei_norm = args.imei_normalize

    # process existing
    for p in sorted(in_dir.glob('*.json')):
        process_input_file(p, out_dir, processed_dir, imei_normalize=imei_norm)

    if args.watch:
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler
        except Exception:
            logger.warning('watchdog not available; watch disabled')
            return
        class Handler(FileSystemEventHandler):
            def on_created(self, event):
                if event.is_directory:
                    return
                path = Path(event.src_path)
                if path.suffix.lower() == '.json':
                    time.sleep(0.2)
                    process_input_file(path, out_dir, processed_dir, imei_normalize=imei_norm)
        obs = Observer()
        obs.schedule(Handler(), str(in_dir), recursive=False)
        obs.start()
        logger.info('Watching for new files...')
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            obs.stop()
            obs.join()

if __name__ == '__main__':
    main()
