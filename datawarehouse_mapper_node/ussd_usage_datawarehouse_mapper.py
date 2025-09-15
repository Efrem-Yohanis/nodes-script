import json
import time
import shutil
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from decimal import Decimal, InvalidOperation

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ussd_cdr_mapper")

# Canonical ordered list (reuse same final layout expected)
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


def enforce_canonical(mapped: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    numeric_fields = {"EL_DEBIT_AMOUNT","EL_ON_NET_INDICATOR","EL_TAX1","EL_TAX2","EL_UNLTD_ROUNDED_UNITS_CHARGED","EL_UNLTD_TOTAL_TIME_CHARGED"}
    for i in range(1,6):
        numeric_fields.add(f"EL_CUR_BALANCE{i}")
        numeric_fields.add(f"EL_CHG_BALANCE{i}")
        numeric_fields.add(f"EL_BUCKET_CUR_BALANCE{i}")
        numeric_fields.add(f"EL_BUCKET_CHG_BALANCE{i}")
    for key in CANONICAL_FIELDS:
        if key in mapped and mapped[key] is not None and mapped[key] != "":
            out[key] = mapped[key]
        else:
            out[key] = 0.0 if key in numeric_fields else ""
    return out


def validate_canonical_record(rec: Dict[str, Any]) -> Tuple[bool, str]:
    if list(rec.keys()) != CANONICAL_FIELDS:
        return False, f"keys mismatch: expected {len(CANONICAL_FIELDS)} fields, got {len(rec.keys())}"
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
        if str(event_label) in ("25",):
            for s in subs:
                if s["type"] == "1":
                    calling = s["data"]
                    break
        if str(event_label) in ("26",):
            for s in subs:
                if s["type"] == "1":
                    called = s["data"]
                    break
    except Exception:
        pass
    return calling, called


def map_ussd(cdr_json: Dict[str, Any]) -> Dict[str, Any]:
    generic = safe_get(cdr_json, ["original","payload","genericRecord"], {}) or cdr_json
    record_elems = safe_get(generic, ["recordElements"], {}) or {}
    extensions = safe_get(generic, ["recordExtensions"], []) or []
    cbl = cdr_json.get("CBL_TAG") or {}

    out = {}
    out["EL_CDR_ID"] = record_elems.get("sessionId") or ""
    out["EL_SRC_CDR_ID"] = record_elems.get("sessionSequenceNumber") or ""
    out["EL_CUST_LOCAL_START_DATE"] = record_elems.get("sessionStartTime") or ""
    out["EL_SESSION_ID"] = record_elems.get("sessionId") or ""

    # find listOfMscc
    list_of_mscc_ext = None
    for ext in extensions:
        if safe_get(ext, ["recordProperty"]) == "listOfMscc":
            list_of_mscc_ext = ext
            break

    # actual usage/rate usage try to read totalTimeConsumed in mscc if present
    el_actual_usage = None
    el_rate_usage = None
    if list_of_mscc_ext:
        for mscc in safe_get(list_of_mscc_ext, ["recordSubExtensions"], []) or []:
            if safe_get(mscc, ["recordProperty"]) == "mscc":
                elems = safe_get(mscc, ["recordElements"], {}) or {}
                tv = elems.get("totalTimeConsumed") or elems.get("timeUsage")
                if tv is not None:
                    d = to_decimal(tv)
                    if d is not None:
                        el_actual_usage = fmt_decimal_to_float(d)
                        el_rate_usage = el_actual_usage
                        break
    out["EL_ACTUAL_USAGE"] = el_actual_usage if el_actual_usage is not None else 0.0
    out["EL_RATE_USAGE"] = el_rate_usage if el_rate_usage is not None else 0.0

    # debit amount and free units
    el_debit_amt = None
    el_free_units = None
    account_blocks = []
    bucket_blocks = []
    if list_of_mscc_ext:
        for mscc in safe_get(list_of_mscc_ext, ["recordSubExtensions"], []) or []:
            if safe_get(mscc, ["recordProperty"]) != "mscc":
                continue
            for sub in safe_get(mscc, ["recordSubExtensions"], []) or []:
                if safe_get(sub, ["recordProperty"]) == "deviceInfo":
                    for s2 in safe_get(sub, ["recordSubExtensions"], []) or []:
                        if safe_get(s2, ["recordProperty"]) == "subscriptionInfo":
                            acct_info = None
                            buckets = []
                            for s3 in safe_get(s2, ["recordSubExtensions"], []) or []:
                                if safe_get(s3, ["recordProperty"]) == "chargingServiceInfo":
                                    for s4 in safe_get(s3, ["recordSubExtensions"], []) or []:
                                        if safe_get(s4, ["recordProperty"]) == "accountInfo" and acct_info is None:
                                            acct_info = safe_get(s4, ["recordElements"], {}) or {}
                                        if safe_get(s4, ["recordProperty"]) == "bucketInfo":
                                            be = safe_get(s4, ["recordElements"], {}) or {}
                                            buckets.append({
                                                "bucketName": be.get("bucketName") or "",
                                                "bucketUnitType": be.get("bucketUnitType") or "",
                                                "bucketBalanceBefore": to_decimal(be.get("bucketBalanceBefore")),
                                                "bucketBalanceAfter": to_decimal(be.get("bucketBalanceAfter")),
                                                "bucketCommitedUnits": to_decimal(be.get("bucketCommitedUnits") or be.get("bucketCommittedUnits")),
                                                "rateId": be.get("rateId") or "",
                                                "committedTaxAmount": be.get("committedTaxAmount")
                                            })
                                        if safe_get(s4, ["recordProperty"]) == "noCharge":
                                            nocharge_elems = safe_get(s4, ["recordElements"], {}) or {}
                                            if el_free_units is None:
                                                el_free_units = to_decimal(nocharge_elems.get("noChargeCommittedUnits"))
                            if acct_info:
                                account_blocks.append({"bundleName": safe_get(s2, ["recordElements","bundleName"]) or "", "acct": acct_info})
                            if buckets:
                                bucket_blocks.append({"bundleName": safe_get(s2, ["recordElements","bundleName"]) or "", "buckets": buckets})
    # compute debit amount
    if account_blocks:
        for ab in account_blocks:
            acct = ab.get("acct") or {}
            v = acct.get("accountBalanceCommittedBR") or acct.get("accountBalanceCommitted")
            if v is not None:
                dv = to_decimal(v)
                if dv is not None:
                    el_debit_amt = fmt_decimal_to_float(dv)
                    break
    if el_debit_amt is None and account_blocks:
        acct = account_blocks[0].get("acct") or {}
        bef = to_decimal(acct.get("accountBalanceBefore"))
        aft = to_decimal(acct.get("accountBalanceAfter"))
        if bef is not None and aft is not None:
            el_debit_amt = fmt_decimal_to_float(bef - aft)
    out["EL_DEBIT_AMOUNT"] = el_debit_amt if el_debit_amt is not None else 0.0
    out["EL_FREE_UNIT_AMOUNT_OF_DURATION"] = fmt_decimal_to_float(el_free_units) if el_free_units is not None else ""

    # populate first 5 account slots
    for idx in range(5):
        n = idx + 1
        if idx < len(account_blocks):
            acct = account_blocks[idx].get("acct") or {}
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

    # populate first 5 bucket slots
    for idx in range(5):
        n = idx + 1
        if idx < len(bucket_blocks):
            entry = bucket_blocks[idx]
            bundle = entry.get("bundleName") or ""
            buckets = entry.get("buckets") or []
            names = [b.get("bucketName") for b in buckets if b.get("bucketName")]
            id_val = (bundle + "-" + ",".join(names)) if bundle and names else (bundle or ",".join(names))
            out[f"EL_BUCKET_BALANCE_ID{n}"] = id_val
            out[f"EL_BUCKET_BALANCE_TYPE{n}"] = ",".join([b.get("bucketUnitType") for b in buckets if b.get("bucketUnitType")])
            cur_vals = []
            for b in buckets:
                ba = b.get("bucketBalanceAfter")
                cur_vals.append(str(fmt_decimal_to_float(ba)) if ba is not None else "0.0")
            out[f"EL_BUCKET_CUR_BALANCE{n}"] = ",".join(cur_vals)
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

    # calling/called
    out["EL_CALLING_PARTY_NUMBER"] = record_elems.get("originatorAddress") or ""
    out["EL_CALLED_PARTY_NUMBER"] = record_elems.get("recipientAddress") or ""

    # IMSI based on event label
    event_label = None
    if isinstance(cbl, dict) and cbl.get("EL_EVENT_LABEL_VAL") is not None:
        event_label = cbl.get("EL_EVENT_LABEL_VAL")
    if event_label is None:
        event_label = record_elems.get("EL_EVENT_LABEL_VAL") or record_elems.get("eventLabel")
    calling_imsi, called_imsi = extract_imsi_from_extensions(extensions, event_label)
    out["EL_CALLING_PARTY_IMSI"] = calling_imsi
    out["EL_CALLED_PARTY_IMSI"] = called_imsi

    # service flow
    el_service_flow = ""
    if list_of_mscc_ext:
        for mscc in safe_get(list_of_mscc_ext, ["recordSubExtensions"], []) or []:
            if safe_get(mscc, ["recordProperty"]) == "mscc":
                elems = safe_get(mscc, ["recordElements"], {}) or {}
                el_service_flow = elems.get("subRecordEventType") or el_service_flow
                break
    out["EL_SERVICE_FLOW"] = el_service_flow or (record_elems.get("subRecordEventType") or "USSD")

    # locations
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
    out["EL_IMEI"] = record_elems.get("userEquipmentValue") or record_elems.get("imei") or ""
    out["EL_REFUND_INDICATOR"] = ""

    # main offering
    main_off = ""
    if account_blocks:
        main_off = account_blocks[0].get("bundleName") or ""
    out["EL_MAIN_OFFERING_ID"] = main_off

    out["EL_CHARGING_PARTY_NUMBER"] = out.get("EL_CALLING_PARTY_NUMBER") or ""
    out["EL_CHARGE_PARTY_IND"] = ""
    out["EL_PAY_TYPE"] = (record_elems.get("EL_PRE_POST") or record_elems.get("prePost") or (cbl.get("EL_PRE_POST") if isinstance(cbl, dict) else ""))
    on_net = record_elems.get("isOnNet")
    if isinstance(on_net, bool):
        out["EL_ON_NET_INDICATOR"] = 1 if on_net else 0
    elif isinstance(on_net, str):
        out["EL_ON_NET_INDICATOR"] = 1 if on_net.lower() == "true" else 0
    else:
        out["EL_ON_NET_INDICATOR"] = 0
    out["EL_ROAM_STATE"] = record_elems.get("RoamingStatus") or record_elems.get("roamingIndicator") or ""
    out["EL_OPPOSE_NETWORK_TYPE"] = record_elems.get("rATType") or record_elems.get("ratType") or ""

    out["EL_CALLING_VPN_TOP_GROUP_NUMBER"] = ""
    out["EL_CALLING_VPN_GROUP_NUMBER"] = ""
    out["EL_CALLING_VPN_SHORT_NUMBERs"] = ""
    out["EL_CALLED_VPN_TOP_GROUP_NUMBER"] = ""
    out["EL_CALLED_VPN_GROUP_NUMBER"] = ""
    out["EL_CALLED_VPN_SHORT_NUMBER"] = ""
    out["EL_LAST_EFFECT_OFFERING"] = ""

    # alternate ids
    alt_ids = []
    if list_of_mscc_ext:
        for mscc in safe_get(list_of_mscc_ext, ["recordSubExtensions"], []) or []:
            for d in safe_get(mscc, ["recordSubExtensions"], []) or []:
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

    # taxes
    el_tax1 = None
    el_tax2 = None
    if account_blocks:
        ca = account_blocks[0].get("acct")
        el_tax1 = to_decimal(ca.get("committedTaxAmount")) if ca else None
    if bucket_blocks:
        first_buckets = bucket_blocks[0].get('buckets', [])
        if first_buckets:
            el_tax2 = first_buckets[0].get('committedTaxAmount') if isinstance(first_buckets[0].get('committedTaxAmount'), Decimal) else to_decimal(first_buckets[0].get('committedTaxAmount'))
    if el_tax1 is None and isinstance(cbl, dict):
        el_tax1 = to_decimal(cbl.get('EL_TAX1'))
    if el_tax2 is None and isinstance(cbl, dict):
        el_tax2 = to_decimal(cbl.get('EL_TAX2'))
    out["EL_TAX1"] = fmt_decimal_to_float(el_tax1) if el_tax1 is not None else 0.0
    out["EL_TAX2"] = fmt_decimal_to_float(el_tax2) if el_tax2 is not None else 0.0

    out["EL_USER_GROUP_ID"] = ""
    out["EL_BUSINESS_TYPE"] = ""
    out["EL_SUBSCRIBER_KEY"] = ""
    out["EL_ACCOUNT_KEY"] = ""
    out["EL_DISCOUNT_OF_LAST_EFF_PROD"] = ""

    # additionalBalanceInfo aggregation
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
                                            adj = None
                                            for s5 in safe_get(s4, ["recordSubExtensions"], []) or []:
                                                if safe_get(s5, ["recordProperty"]) == "adjustBalanceInfo":
                                                    adj = s5
                                                    break
                                            adj_elems = safe_get(adj, ["recordElements"], {}) or {}
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
                                                "chargingServiceName": ab_elems.get("chargingServiceName") or "",
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

    # unlimited bundle detection
    out["EL_UNLTD_BUNDLE_NAME"] = ""
    out["EL_UNLTD_TOTAL_TIME_CHARGED"] = 0.0
    out["EL_UNLTD_ROUNDED_UNITS_CHARGED"] = ""
    out["EL_UNLTD_BUNDLE_UNIT_TYPE"] = ""
    if account_blocks:
        for entry in account_blocks:
            acct = entry.get('acct') or {}
            bundle_name = entry.get('bundleName') or ""
            committed = to_decimal(acct.get('accountBalanceCommitted')) or to_decimal(acct.get('accountBalanceCommittedBR'))
            total_units = to_decimal(acct.get('totalUnitsCharged')) or to_decimal(acct.get('totalTimeCharged'))
            if committed is not None and committed == Decimal(0) and total_units is not None and total_units > Decimal(0):
                out["EL_UNLTD_BUNDLE_NAME"] = bundle_name
                out["EL_UNLTD_TOTAL_TIME_CHARGED"] = fmt_decimal_to_float(total_units) if fmt_decimal_to_float(total_units) is not None else 0.0
                out["EL_UNLTD_BUNDLE_UNIT_TYPE"] = "UNITS"
                break

    # orig location
    orig_loc = record_elems.get("origUserLocationInfo") or record_elems.get("origUserLocation") or ""
    if orig_loc:
        rat_orig = record_elems.get("rATType") or record_elems.get("ratType") or record_elems.get("rAT")
        if str(rat_orig) == "6":
            out["EL_ORIG_LOCATION"] = decode_location_hex_field(orig_loc)
        else:
            tail = last_n_chars(orig_loc, 14)
            if len(tail) >= 14:
                a = tail[0:6]; b = tail[6:10]; c = tail[10:14]
                out["EL_ORIG_LOCATION"] = f"{a}-{b}-{c}"
            else:
                out["EL_ORIG_LOCATION"] = orig_loc
    else:
        out["EL_ORIG_LOCATION"] = ""

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
            m = map_ussd(rec)
            valid, msg = validate_canonical_record(m)
            if valid:
                mapped.append(m)
            else:
                rejects.append({"reason": msg, "record": m})
        except Exception as e:
            logger.exception("Mapping failed")
            rejects.append({"reason": str(e), "record": rec})

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{path.stem}_ussd_phase1.json"
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
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    processed_dir = Path(args.processed_dir)

    for p in sorted(in_dir.glob('*.json')):
        process_input_file(p, out_dir, processed_dir)

if __name__ == '__main__':
    main()
