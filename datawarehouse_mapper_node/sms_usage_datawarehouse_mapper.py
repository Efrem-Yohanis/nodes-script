import os
import json
import time
import shutil
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from decimal import Decimal, InvalidOperation
import binascii
import re

# Simple logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("phase1_mapper")

# --- Helpers ---

def safe_get(node: Any, path: list, default: Any = None) -> Any:
    """Safely walk nested dict/list using a path of keys/indexes."""
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


def to_float_safe(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


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

# --- Phase 1 mapping logic ---

def extract_account_info_from_mscc(mscc_block: dict) -> Dict[str, Any]:
    """Navigate into mscc recordSubExtensions to find accountInfo recordElements dict.

    Returns a dict (may be empty) representing accountInfo.recordElements if found.
    """
    # mscc_block may contain recordSubExtensions, find deviceInfo -> subscriptionInfo -> chargingServiceInfo -> accountInfo
    for sub in mscc_block.get("recordSubExtensions", []) or []:
        if safe_get(sub, ["recordProperty"]) == "deviceInfo":
            for sub2 in safe_get(sub, ["recordSubExtensions"], []) or []:
                if safe_get(sub2, ["recordProperty"]) == "subscriptionInfo":
                    for sub3 in safe_get(sub2, ["recordSubExtensions"], []) or []:
                        if safe_get(sub3, ["recordProperty"]) == "chargingServiceInfo":
                            for sub4 in safe_get(sub3, ["recordSubExtensions"], []) or []:
                                if safe_get(sub4, ["recordProperty"]) == "accountInfo":
                                    return safe_get(sub4, ["recordElements"], {}) or {}
    return {}


def extract_nocharge_units_from_mscc(mscc_block: dict) -> Optional[float]:
    """Find noChargeCommittedUnits under chargingServiceInfo -> noCharge if present."""
    for sub in mscc_block.get("recordSubExtensions", []) or []:
        if safe_get(sub, ["recordProperty"]) == "deviceInfo":
            for sub2 in safe_get(sub, ["recordSubExtensions"], []) or []:
                if safe_get(sub2, ["recordProperty"]) == "subscriptionInfo":
                    for sub3 in safe_get(sub2, ["recordSubExtensions"], []) or []:
                        if safe_get(sub3, ["recordProperty"]) == "chargingServiceInfo":
                            for sub4 in safe_get(sub3, ["recordSubExtensions"], []) or []:
                                if safe_get(sub4, ["recordProperty"]) == "noCharge":
                                    elems = safe_get(sub4, ["recordElements"], {}) or {}
                                    val = elems.get("noChargeCommittedUnits")
                                    return to_float_safe(val)
    return None


def map_phase1(cdr_json: dict) -> Dict[str, Any]:
    """Apply Phase 1, Phase 2, Phase 3 and Phase 4 mapping rules and return a map with EL_* fields.

    Phase 1: session-level mappings (1-5).
    Phase 2: accountInfo (up to 5) and bucketInfo (up to 5) mappings.
    Phase 3: IMSI, location decoding, calling/called numbers and other fields.
    Phase 4: additionalBalanceInfo iteration (up to 5 entries).
    """
    # locate the genericRecord node
    generic = safe_get(cdr_json, ["original", "payload", "genericRecord"], {}) or {}
    if not generic:
        # fallback: maybe the top-level is already genericRecord
        generic = cdr_json.get("genericRecord") or cdr_json

    record_elems = safe_get(generic, ["recordElements"], {}) or {}

    # initialize common vars early to avoid UnboundLocalError when referenced later
    el_rat = record_elems.get("rATType") or record_elems.get("ratType") or ""
    el_imei = record_elems.get("userEquipmentValue") or record_elems.get("imei") or ""
    user_loc = record_elems.get("userLocationInformation") or ""

    # 1,2,3 direct mappings
    el_cdr_id = record_elems.get("sessionId")
    el_src_cdr_id = record_elems.get("sessionSequenceNumber")
    el_cust_local_start_date = record_elems.get("sessionStartTime")

    # 4 EL_DEBIT_AMOUNT: priority of fields inside listOfMscc.mscc -> accountInfo
    el_debit_amount: Optional[float] = None

    # find listOfMscc extension
    el_extensions = safe_get(generic, ["recordExtensions"], []) or []
    list_of_mscc = None
    for ext in el_extensions:
        if safe_get(ext, ["recordProperty"]) == "listOfMscc":
            list_of_mscc = ext
            break

    # collect mscc blocks
    mscc_blocks = []
    if list_of_mscc:
        for mscc in safe_get(list_of_mscc, ["recordSubExtensions"], []) or []:
            if safe_get(mscc, ["recordProperty"]) == "mscc":
                mscc_blocks.append(mscc)

    # Determine EL_DEBIT_AMOUNT using accountInfo within mscc blocks
    if mscc_blocks:
        for mscc in mscc_blocks:
            account_info = extract_account_info_from_mscc(mscc)
            # use Decimal for monetary math
            val = account_info.get("accountBalanceCommittedBR")
            if val is not None:
                dv = to_decimal(val)
                if dv is not None:
                    el_debit_amount = fmt_decimal_to_float(dv)
                    break
            val = account_info.get("accountBalanceCommitted")
            if val is not None:
                dv = to_decimal(val)
                if dv is not None:
                    el_debit_amount = fmt_decimal_to_float(dv)
                    break
            before = to_decimal(account_info.get("accountBalanceBefore"))
            after = to_decimal(account_info.get("accountBalanceAfter"))
            if before is not None and after is not None:
                diff = before - after
                el_debit_amount = fmt_decimal_to_float(diff)
                break

    # 5 EL_FREE_UNIT_AMOUNT_OF_DURATION
    el_free_units = None
    if mscc_blocks:
        for mscc in mscc_blocks:
            nocharge = extract_nocharge_units_from_mscc(mscc)
            if nocharge is not None:
                el_free_units = nocharge
                break

    # --- Phase 2: accountInfo and bucketInfo loops (up to 5 each) ---
    account_blocks = []  # list of dicts (recordElements) for accountInfo
    bucket_blocks = []   # list of dicts (recordElements) for bucketInfo
    alt_ids = []
    main_offering = ""
    tax1_vals = []
    tax2_vals = []

    for mscc in mscc_blocks:
        # accountInfo
        acc = extract_account_info_from_mscc(mscc)
        if acc:
            account_blocks.append(acc)
            # tax1: committedTaxAmount
            t = acc.get("committedTaxAmount")
            if t is not None:
                tv = to_float_safe(t)
                if tv is not None:
                    tax1_vals.append(tv)
        # subscriptionInfo search for alternateId, bundleName and bucketInfo
        for sub in safe_get(mscc, ["recordSubExtensions"], []) or []:
            if safe_get(sub, ["recordProperty"]) == "deviceInfo":
                for s2 in safe_get(sub, ["recordSubExtensions"], []) or []:
                    if safe_get(s2, ["recordProperty"]) == "subscriptionInfo":
                        sub_elems = safe_get(s2, ["recordElements"], {}) or {}
                        alt = sub_elems.get("alternateId")
                        if alt:
                            alt_ids.append(str(alt))
                        bn = sub_elems.get("bundleName")
                        if bn and not main_offering:
                            main_offering = str(bn)
                        for s3 in safe_get(s2, ["recordSubExtensions"], []) or []:
                            if safe_get(s3, ["recordProperty"]) == "chargingServiceInfo":
                                for s4 in safe_get(s3, ["recordSubExtensions"], []) or []:
                                    if safe_get(s4, ["recordProperty"]) == "bucketInfo":
                                        elems = safe_get(s4, ["recordElements"], {}) or {}
                                        bucket_blocks.append(elems)
                                        tb = safe_get(s4, ["recordElements", "committedTaxAmount"])
                                        tbv = to_float_safe(tb)
                                        if tbv is not None:
                                            tax2_vals.append(tbv)

    # --- Phase 3 extra fields ---
    # Calling/called numbers
    el_calling = record_elems.get("originatorAddress") or record_elems.get("callingParty") or record_elems.get("originationAddress") or ""
    el_called = record_elems.get("recipientAddress") or record_elems.get("calledParty") or ""

    # IMSI extraction from listOfSubscriptionID based on EL_EVENT_LABEL_VAL
    el_event_label = record_elems.get("EL_EVENT_LABEL_VAL") or record_elems.get("eventLabel")
    calling_imsi = ""
    called_imsi = ""
    # find listOfSubscriptionID extension
    sublist_ext = None
    for ext in el_extensions:
        if safe_get(ext, ["recordProperty"]) == "listOfSubscriptionID":
            sublist_ext = ext
            break
    sub_ids = []
    if sublist_ext:
        for sid_block in safe_get(sublist_ext, ["recordSubExtensions"], []) or []:
            if safe_get(sid_block, ["recordProperty"]) == "subscriptionId":
                elems = safe_get(sid_block, ["recordElements"], {}) or {}
                dtype = elems.get("subscriptionIDType") or elems.get("subscriptionIdType")
                ddata = elems.get("subscriptionIDData") or elems.get("subscriptionIdData")
                if ddata is not None:
                    data_str = str(ddata)
                    # normalize imsi- prefix (e.g. 'imsi-63602...')
                    if data_str.lower().startswith("imsi-"):
                        data_str = data_str.split('-', 1)[1]
                    # accept common IMSI type codes (1) and vendor-specific (102 etc.)
                    type_str = str(dtype) if dtype is not None else ""
                    sub_ids.append({"type": type_str, "data": data_str})
    # decide IMSIs
    try:
        # look for type '1' first, fall back to types like '102' or any entry where data looks like IMSI
        if str(el_event_label) == "25":
            for x in sub_ids:
                if x.get("type") == "1" or x.get("type") == "102" or x.get("data", "").isdigit():
                    calling_imsi = x["data"]
                    break
        elif str(el_event_label) == "26":
            for x in sub_ids:
                if x.get("type") == "1" or x.get("type") == "102" or x.get("data", "").isdigit():
                    called_imsi = x["data"]
                    break
    except Exception:
        pass

    # Service flow: subRecordEventType in mscc.recordElements
    el_service_flow = ""
    if mscc_blocks:
        first_mscc_elems = safe_get(mscc_blocks[0], ["recordElements"], {}) or {}
        el_service_flow = first_mscc_elems.get("subRecordEventType") or ""

    # use IMEI helper
    if el_imei:
        el_imei = imei_from_user_equipment_value(el_imei)

    # initialize calling/called location defaults
    el_calling_loc = ""
    el_called_loc = ""

    # decode calling/called location using helper when possible
    if user_loc:
        # if rat indicates hex format use decode helper
        if str(el_rat) == "6":
            el_calling_loc = decode_location_hex_field(user_loc)
            el_called_loc = el_calling_loc
        else:
            tail = last_n_chars(user_loc, 13)
            if len(tail) >= 13:
                a = tail[0:5]
                b = tail[5:9]
                c = tail[9:13]
                el_calling_loc = f"{a}-{b}-{c}"
                el_called_loc = el_calling_loc
            else:
                el_calling_loc = tail
                el_called_loc = tail

    # direct mappings and defaults
    el_send_result = record_elems.get("resultCode") or ""
    el_refund_indicator = ""
    el_main_offering_id = main_offering or ""
    el_charging_party_number = el_calling
    el_charge_party_ind = ""
    # EL_PAY_TYPE: prefer recordElements, fallback to CBL_TAG at file level
    el_pay_type = record_elems.get("EL_PRE_POST") or record_elems.get("prePost") or safe_get(cdr_json, ["CBL_TAG", "EL_PRE_POST"]) or ""
    el_on_net = record_elems.get("isOnNet")
    if isinstance(el_on_net, bool):
        el_on_net = 1 if el_on_net else 0
    elif isinstance(el_on_net, str):
        el_on_net = 1 if el_on_net.lower() == "true" else 0
    else:
        el_on_net = ""

    # Roaming status: prefer RoamingStatus, fallback to roamingIndicator used in some feeds
    el_roam_state = record_elems.get("RoamingStatus") or record_elems.get("roamingIndicator") or ""
    el_rat = record_elems.get("rATType") or record_elems.get("ratType") or ""
    el_group_id = record_elems.get("groupID") or ""

    el_alternate_id = "~".join(alt_ids) if alt_ids else ""
    el_user_state = record_elems.get("deviceState") or ""

    # taxes: take first values if present
    el_tax1 = tax1_vals[0] if tax1_vals else 0.0
    el_tax2 = tax2_vals[0] if tax2_vals else 0.0

    # --- Phase 4: additionalBalanceInfo extraction (up to 5 entries) ---
    additional_blocks = []
    if mscc_blocks:
        for mscc in mscc_blocks:
            for sub in safe_get(mscc, ["recordSubExtensions"], []) or []:
                if safe_get(sub, ["recordProperty"]) == "deviceInfo":
                    for s2 in safe_get(sub, ["recordSubExtensions"], []) or []:
                        if safe_get(s2, ["recordProperty"]) == "subscriptionInfo":
                            for s3 in safe_get(s2, ["recordSubExtensions"], []) or []:
                                if safe_get(s3, ["recordProperty"]) == "chargingServiceInfo":
                                    for s4 in safe_get(s3, ["recordSubExtensions"], []) or []:
                                        if safe_get(s4, ["recordProperty"]) == "additionalBalanceInfo":
                                            # chargingServiceName may be in recordElements of s4
                                            ab_elems = safe_get(s4, ["recordElements"], {}) or {}
                                            chargingName = ab_elems.get("chargingServiceName") or ab_elems.get("chargingservicename") or ""
                                            # adjustBalanceInfo may be a nested subExtension
                                            adj = None
                                            for s5 in safe_get(s4, ["recordSubExtensions"], []) or []:
                                                if safe_get(s5, ["recordProperty"]) == "adjustBalanceInfo":
                                                    adj = s5
                                                    break
                                            adj_elems = safe_get(adj, ["recordElements"], {}) or {}
                                            # bucketInfo may be nested under adj subExtensions or directly under additionalBalanceInfo
                                            bucket_info = safe_get(adj, ["recordSubExtensions"], []) or []
                                            bucket_elems = {}
                                            # try adj -> bucketInfo
                                            if adj:
                                                for b in safe_get(adj, ["recordSubExtensions"], []) or []:
                                                    if safe_get(b, ["recordProperty"]) == "bucketInfo":
                                                        bucket_elems = safe_get(b, ["recordElements"], {}) or {}
                                                        break
                                            # fallback: additionalBalanceInfo.recordSubExtensions -> bucketInfo
                                            if not bucket_elems:
                                                for b in safe_get(s4, ["recordSubExtensions"], []) or []:
                                                    if safe_get(b, ["recordProperty"]) == "bucketInfo":
                                                        bucket_elems = safe_get(b, ["recordElements"], {}) or {}
                                                        break

                                            # also some fields live directly under additionalBalanceInfo.recordElements
                                            direct_bucket_committed = ab_elems.get("bucketCommitedUnits") or ab_elems.get("bucketCommittedUnits")

                                            additional_blocks.append({
                                                "chargingServiceName": chargingName,
                                                "usageType": adj_elems.get("usageType") or "",
                                                "usedAs": adj_elems.get("usedAs") or "",
                                                "bucketName": bucket_elems.get("bucketName") or "",
                                                "bucketUnitType": bucket_elems.get("bucketUnitType") or "",
                                                "bucketKindOfUnit": bucket_elems.get("bucketKindOfUnit") or "",
                                                "bucketBalanceBefore": to_float_safe(bucket_elems.get("bucketBalanceBefore")),
                                                "bucketBalanceAfter": to_float_safe(bucket_elems.get("bucketBalanceAfter")),
                                                "carryOverBucket": bucket_elems.get("carryOverBucket") or "",
                                                "bucketCommitedUnits": to_float_safe(bucket_elems.get("bucketCommitedUnits") or direct_bucket_committed),
                                                "bucketReservedUnits": to_float_safe(bucket_elems.get("bucketReservedUnits")),
                                                "rateId": bucket_elems.get("rateId") or "",
                                                "primaryCostCommitted": to_float_safe(bucket_elems.get("primaryCostCommitted")),
                                                "secondaryCostCommitted": to_float_safe(bucket_elems.get("secondaryCostCommitted")),
                                                "taxationID": bucket_elems.get("taxationID") or bucket_elems.get("taxationId") or "",
                                                "taxRateApplied": to_float_safe(bucket_elems.get("taxRateApplied")),
                                                "committedTaxAmount": to_float_safe(bucket_elems.get("committedTaxAmount")),
                                                "totalTaxAmount": to_float_safe(bucket_elems.get("totalTaxAmount")),
                                                "tariffID": bucket_elems.get("tariffID") or bucket_elems.get("tariffId") or "",
                                                "totalUnitsCharged": to_float_safe(bucket_elems.get("totalUnitsCharged"))
                                            })

    # Prepare base output
    out = {
        "EL_CDR_ID": el_cdr_id or "",
        "EL_SRC_CDR_ID": el_src_cdr_id or "",
        "EL_CUST_LOCAL_START_DATE": el_cust_local_start_date or "",
        "EL_DEBIT_AMOUNT": el_debit_amount if el_debit_amount is not None else 0.0,
        "EL_FREE_UNIT_AMOUNT_OF_DURATION": el_free_units if el_free_units is not None else "",
        "EL_CALLING_PARTY_NUMBER": el_calling,
        "EL_CALLED_PARTY_NUMBER": el_called,
        "EL_CALLING_PARTY_IMSI": calling_imsi,
        "EL_CALLED_PARTY_IMSI": called_imsi,
        "EL_SERVICE_FLOW": el_service_flow,
        "EL_CALLING_LOCATION_INFO": el_calling_loc,
        "EL_CALLED_LOCATION_INFO": el_called_loc,
        "EL_SEND_RESULT": el_send_result,
        "EL_IMEI": el_imei,
        "EL_REFUND_INDICATOR": el_refund_indicator,
        "EL_MAIN_OFFERING_ID": el_main_offering_id,
        "EL_CHARGING_PARTY_NUMBER": el_charging_party_number,
        "EL_CHARGE_PARTY_IND": el_charge_party_ind,
        "EL_PAY_TYPE": el_pay_type,
        "EL_ON_NET_INDICATOR": el_on_net,
        "EL_ROAM_STATE": el_roam_state,
        "EL_OPPOSE_NETWORK_TYPE": el_rat,
        "EL_CALLING_VPN_TOP_GROUP_NUMBER": el_group_id,
        "EL_CALLING_VPN_GROUP_NUMBER": "",
        "EL_CALLING_VPN_SHORT_NUMBERs": "",
        "EL_CALLED_VPN_TOP_GROUP_NUMBER": "",
        "EL_CALLED_VPN_GROUP_NUMBER": "",
        "EL_CALLED_VPN_SHORT_NUMBER": "",
        "EL_LAST_EFFECT_OFFERING": "",
        "EL_ALTERNATE_ID": el_alternate_id,
        "EL_HOME_ZONE_ID": "",
        "EL_USER_STATE": el_user_state,
        "EL_PAY_DEFAULT_ACCT_ID": "",
        "EL_TAX1": el_tax1,
        "EL_TAX2": el_tax2,
        "EL_USER_GROUP_ID": "",
        "EL_BUSINESS_TYPE": "",
        "EL_SUBSCRIBER_KEY": "",
        "EL_ACCOUNT_KEY": "",
        "EL_DISCOUNT_OF_LAST_EFF_PROD": ""
    }

    # Fill Phase 2 account and bucket fields (up to 5)
    for idx in range(5):
        n = idx + 1
        acct = account_blocks[idx] if idx < len(account_blocks) else {}
        out[f"EL_ACCT_BALANCE_ID{n}"] = acct.get("accountID") if acct.get("accountID") is not None else ""
        out[f"EL_BALANCE_TYPE{n}"] = acct.get("accountType") if acct.get("accountType") is not None else ""
        # use Decimal for balance values
        cur_d = to_decimal(acct.get("accountBalanceAfter"))
        cur_v = fmt_decimal_to_float(cur_d)
        out[f"EL_CUR_BALANCE{n}"] = cur_v if cur_v is not None else 0.0
        before_d = to_decimal(acct.get("accountBalanceBefore"))
        if before_d is not None and cur_d is not None:
            diff = before_d - (cur_d)
            out[f"EL_CHG_BALANCE{n}"] = fmt_decimal_to_float(diff) if fmt_decimal_to_float(diff) is not None else 0.0
        else:
            out[f"EL_CHG_BALANCE{n}"] = 0.0
        out[f"EL_RATE_ID{n}"] = acct.get("rateId") if acct.get("rateId") is not None else ""

    for idx in range(5):
        n = idx + 1
        b = bucket_blocks[idx] if idx < len(bucket_blocks) else {}
        out[f"EL_BUCKET_BALANCE_ID{n}"] = b.get("bucketName") if b.get("bucketName") is not None else ""
        out[f"EL_BUCKET_BALANCE_TYPE{n}"] = b.get("bucketUnitType") if b.get("bucketUnitType") is not None else ""
        curb = to_decimal(b.get("bucketBalanceAfter"))
        curb_f = fmt_decimal_to_float(curb)
        out[f"EL_BUCKET_CUR_BALANCE{n}"] = curb_f if curb_f is not None else 0.0
        beforeb = to_decimal(b.get("bucketBalanceBefore"))
        if beforeb is not None and curb is not None:
            diffb = beforeb - curb
            out[f"EL_BUCKET_CHG_BALANCE{n}"] = fmt_decimal_to_float(diffb) if fmt_decimal_to_float(diffb) is not None else 0.0
        else:
            out[f"EL_BUCKET_CHG_BALANCE{n}"] = 0.0
        out[f"EL_BUCKET_RATE_ID{n}"] = b.get("rateId") if b.get("rateId") is not None else ""

    # Fill Phase 4 additionalBalanceInfo fields (up to 5)
    # Phase 4: aggregate additionalBalanceInfo entries into comma-joined single fields (91-110)
    def _join_attr(key):
        parts = []
        for a in additional_blocks:
            v = a.get(key)
            if v is None or v == "":
                continue
            parts.append(str(v))
        return ",".join(parts)

    out["EL_ADDITIONALBALANCEINFO_CHARGINGSERVICENAME"] = _join_attr("chargingServiceName")
    out["EL_ADDITIONALBALANCEINFO_USAGETYPE"] = _join_attr("usageType")
    out["EL_ADDITIONALBALANCEINFO_USEDAS"] = _join_attr("usedAs")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETNAME"] = _join_attr("bucketName")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETUNITTYPE"] = _join_attr("bucketUnitType")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETKINDOFUNIT"] = _join_attr("bucketKindOfUnit")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEBEFORE"] = _join_attr("bucketBalanceBefore")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEAFTER"] = _join_attr("bucketBalanceAfter")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_CARRYOVERBUCKET"] = _join_attr("carryOverBucket")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETCOMMITEDUNITS"] = _join_attr("bucketCommitedUnits")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETRESERVEDUNITS"] = _join_attr("bucketReservedUnits")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_RATEID"] = _join_attr("rateId")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_PRIMARYCOSTCOMMITTED"] = _join_attr("primaryCostCommitted")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_SECONDARYCOSTCOMMITTED"] = _join_attr("secondaryCostCommitted")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXATIONID"] = _join_attr("taxationID")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXRATEAPPLIED"] = _join_attr("taxRateApplied")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_COMMITTEDTAXAMOUNT"] = _join_attr("committedTaxAmount")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALTAXAMOUNT"] = _join_attr("totalTaxAmount")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TARIFFID"] = _join_attr("tariffID")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALUNITSCHARGED"] = _join_attr("totalUnitsCharged")

    # --- Phase 5: Unlimited bundle detection and original location decoding ---
    # Detect unlimited bundle: for each subscriptionInfo (represented by account_blocks and bucket_blocks aligned by mscc/subscription)
    el_unltd_bundle_name = ""
    el_unltd_rounded_units_charged = ""
    el_unltd_bundle_unit_type = ""

    # We need to inspect subscription-level elements. We will iterate mscc_blocks and their subscriptionInfo blocks again
    if mscc_blocks:
        for mscc in mscc_blocks:
            for sub in safe_get(mscc, ["recordSubExtensions"], []) or []:
                if safe_get(sub, ["recordProperty"]) == "deviceInfo":
                    for s2 in safe_get(sub, ["recordSubExtensions"], []) or []:
                        if safe_get(s2, ["recordProperty"]) == "subscriptionInfo":
                            sub_elems = safe_get(s2, ["recordElements"], {}) or {}
                            bundle_name = sub_elems.get("bundleName") or sub_elems.get("bundle_name")
                            # find chargingServiceInfo under this subscriptionInfo
                            for s3 in safe_get(s2, ["recordSubExtensions"], []) or []:
                                if safe_get(s3, ["recordProperty"]) == "chargingServiceInfo":
                                    # check for any bucketInfo under this chargingServiceInfo
                                    has_bucket = False
                                    for s4 in safe_get(s3, ["recordSubExtensions"], []) or []:
                                        if safe_get(s4, ["recordProperty"]) == "bucketInfo":
                                            has_bucket = True
                                            break
                                    # find accountInfo elements
                                    account_info_elems = {}
                                    for s4 in safe_get(s3, ["recordSubExtensions"], []) or []:
                                        if safe_get(s4, ["recordProperty"]) == "accountInfo":
                                            account_info_elems = safe_get(s4, ["recordElements"], {}) or {}
                                            break
                                    acct_committed = to_float_safe(account_info_elems.get("accountBalanceCommitted"))
                                    total_units = to_float_safe(account_info_elems.get("totalUnitsCharged"))
                                    # Condition: no bucketInfo, accountBalanceCommitted == 0, totalUnitsCharged > 0
                                    if (not has_bucket) and (acct_committed is not None and acct_committed == 0) and (total_units is not None and total_units > 0):
                                        if bundle_name:
                                            el_unltd_bundle_name = str(bundle_name)
                                            el_unltd_rounded_units_charged = total_units
                                            el_unltd_bundle_unit_type = "UNITS"
                                            # break out when first match found
                                            break
                            if el_unltd_bundle_name:
                                break
                    if el_unltd_bundle_name:
                        break
            if el_unltd_bundle_name:
                break

    # EL_ORIG_LOCATION decoding per Phase 5
    el_orig_location = ""
    orig_user_loc = record_elems.get("origUserLocationInfo") or record_elems.get("origUserLocationInformation") or record_elems.get("userLocationInformation")
    try:
        rat_val = record_elems.get("rATType") or record_elems.get("ratType")
        if rat_val is None:
            rat_val = record_elems.get("rAT") if "rAT" in record_elems else None
    except Exception:
        rat_val = None

    def _swap_pairs_and_remove_f(hex6: str) -> str:
        # hex6 expected length 6 -> three pairs; reverse each pair and join, then remove 'F'
        if not isinstance(hex6, str) or len(hex6) != 6:
            return hex6
        parts = [hex6[i:i+2] for i in range(0, 6, 2)]
        swapped = ''.join(p[::-1] for p in parts)
        return swapped.replace('F', '').replace('f', '')

    if orig_user_loc:
        s = str(orig_user_loc)
        if str(rat_val) == "6":
            tail = s[-18:]
            if len(tail) == 18:
                tac_hex = tail[0:4]
                mccmnc_hex = tail[4:10]
                eci_hex = tail[10:18]
                try:
                    tac_dec = str(int(tac_hex, 16))
                except Exception:
                    tac_dec = tac_hex
                mccmnc = _swap_pairs_and_remove_f(mccmnc_hex)
                try:
                    eci_dec = int(eci_hex, 16)
                    eNodeB = str(eci_dec % 256)
                    cell_index = str(eci_dec // 256)
                except Exception:
                    eNodeB = eci_hex
                    cell_index = ""
                el_orig_location = f"{mccmnc}-{tac_dec}-{eNodeB}-{cell_index}"
            else:
                # fallback to last-14 behavior
                tail = s[-14:]
                if len(tail) >= 14:
                    a = tail[0:6]
                    b = tail[6:10]
                    c = tail[10:14]
                    el_orig_location = f"{a}-{b}-{c}"
                else:
                    el_orig_location = tail
        else:
            tail = s[-14:]
            if len(tail) >= 14:
                a = tail[0:6]
                b = tail[6:10]
                c = tail[10:14]
                el_orig_location = f"{a}-{b}-{c}"
            else:
                el_orig_location = s[-13:][-13:]

    # attach Phase 5 outputs to out
    out["EL_UNLTD_BUNDLE_NAME"] = el_unltd_bundle_name or ""
    out["EL_UNLTD_ROUNDED_UNITS_CHARGED"] = el_unltd_rounded_units_charged if el_unltd_rounded_units_charged != "" else ""
    out["EL_UNLTD_BUNDLE_UNIT_TYPE"] = el_unltd_bundle_unit_type or ""
    out["EL_ORIG_LOCATION"] = el_orig_location or ""

    # enforce canonical 114-field layout and types
    try:
        canonical = enforce_canonical(out)
    except Exception:
        canonical = out
    return canonical


# --- File processing ---

def read_json_stable(path: Path, retries: int = 5, delay: float = 0.2) -> dict:
    for i in range(retries):
        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            time.sleep(delay)
    raise


def process_input_file(path: Path, out_dir: Path, processed_dir: Path) -> None:
    logger.info(f"Processing file: {path.name}")
    try:
        data = read_json_stable(path)
    except Exception as e:
        logger.error(f"Failed to read {path}: {e}")
        return

    # determine records to map: support single genericRecord or a container with 'records'
    records = []
    if isinstance(data, dict) and "records" in data and isinstance(data["records"], dict):
        records = list(data["records"].values())
    elif isinstance(data, list):
        records = data
    else:
        # treat the whole file as a single record-containing wrapper
        records = [data]

    mapped = []
    for rec in records:
        # ensure exactly the canonical 114 fields per record
        try:
            mapped_rec = map_phase1(rec)
            mapped.append(enforce_canonical(mapped_rec))
        except Exception:
            # fallback to unmodified mapping on error
            mapped.append(map_phase1(rec))

    # write output per input file
    out_path = out_dir / f"{path.stem}_phase1.json"
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".json.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(mapped, f, indent=2, ensure_ascii=False)
        tmp.replace(out_path)
        logger.info(f"Wrote mapped output: {out_path}")
        # move processed
        processed_dir.mkdir(parents=True, exist_ok=True)
        dest = processed_dir / path.name
        shutil.move(str(path), str(dest))
    except Exception as e:
        logger.error(f"Failed to write mapped output for {path.name}: {e}")


# --- CLI / watch loop ---

def ensure_dirs(*dirs):
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)


def main():
    in_dir = Path("in")
    out_dir = Path("out")
    processed_dir = Path("processed")
    ensure_dirs(in_dir, out_dir, processed_dir)

    # process existing files
    for p in sorted(in_dir.glob("*.json")):
        process_input_file(p, out_dir, processed_dir)

    # watch for new files
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler

        class Handler(FileSystemEventHandler):
            def on_created(self, event):
                if event.is_directory:
                    return
                path = Path(event.src_path)
                if path.suffix.lower() == ".json":
                    # small delay to allow writer to finish
                    time.sleep(0.2)
                    process_input_file(path, out_dir, processed_dir)

        obs = Observer()
        obs.schedule(Handler(), str(in_dir), recursive=False)
        obs.start()
        logger.info("Watching ./in for new JSON files...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping")
    except Exception:
        logger.exception("Watch mode unavailable (missing watchdog?). Exiting.")


if __name__ == "__main__":
    main()

# Canonical ordered list of 114 EL_* fields (Phases 1-5) in exact spec order
CANONICAL_FIELDS = [
    # Phase 1 (1-5)
    "EL_CDR_ID",
    "EL_SRC_CDR_ID",
    "EL_CUST_LOCAL_START_DATE",
    "EL_DEBIT_AMOUNT",
    "EL_FREE_UNIT_AMOUNT_OF_DURATION",
    # Phase 2 account fields (6-30) - 5 account slots
    "EL_ACCT_BALANCE_ID1","EL_BALANCE_TYPE1","EL_CUR_BALANCE1","EL_CHG_BALANCE1","EL_RATE_ID1",
    "EL_ACCT_BALANCE_ID2","EL_BALANCE_TYPE2","EL_CUR_BALANCE2","EL_CHG_BALANCE2","EL_RATE_ID2",
    "EL_ACCT_BALANCE_ID3","EL_BALANCE_TYPE3","EL_CUR_BALANCE3","EL_CHG_BALANCE3","EL_RATE_ID3",
    "EL_ACCT_BALANCE_ID4","EL_BALANCE_TYPE4","EL_CUR_BALANCE4","EL_CHG_BALANCE4","EL_RATE_ID4",
    "EL_ACCT_BALANCE_ID5","EL_BALANCE_TYPE5","EL_CUR_BALANCE5","EL_CHG_BALANCE5","EL_RATE_ID5",
    # Phase 2 bucket fields (31-55) - 5 bucket slots
    "EL_BUCKET_BALANCE_ID1","EL_BUCKET_BALANCE_TYPE1","EL_BUCKET_CUR_BALANCE1","EL_BUCKET_CHG_BALANCE1","EL_BUCKET_RATE_ID1",
    "EL_BUCKET_BALANCE_ID2","EL_BUCKET_BALANCE_TYPE2","EL_BUCKET_CUR_BALANCE2","EL_BUCKET_CHG_BALANCE2","EL_BUCKET_RATE_ID2",
    "EL_BUCKET_BALANCE_ID3","EL_BUCKET_BALANCE_TYPE3","EL_BUCKET_CUR_BALANCE3","EL_BUCKET_CHG_BALANCE3","EL_BUCKET_RATE_ID3",
    "EL_BUCKET_BALANCE_ID4","EL_BUCKET_BALANCE_TYPE4","EL_BUCKET_CUR_BALANCE4","EL_BUCKET_CHG_BALANCE4","EL_BUCKET_RATE_ID4",
    "EL_BUCKET_BALANCE_ID5","EL_BUCKET_BALANCE_TYPE5","EL_BUCKET_CUR_BALANCE5","EL_BUCKET_CHG_BALANCE5","EL_BUCKET_RATE_ID5",
    # Phase 3 fields (56-90)
    "EL_CALLING_PARTY_NUMBER","EL_CALLED_PARTY_NUMBER","EL_CALLING_PARTY_IMSI","EL_CALLED_PARTY_IMSI","EL_SERVICE_FLOW",
    "EL_CALLING_LOCATION_INFO","EL_CALLED_LOCATION_INFO","EL_SEND_RESULT","EL_IMEI","EL_REFUND_INDICATOR",
    "EL_MAIN_OFFERING_ID","EL_CHARGING_PARTY_NUMBER","EL_CHARGE_PARTY_IND","EL_PAY_TYPE","EL_ON_NET_INDICATOR",
    "EL_ROAM_STATE","EL_OPPOSE_NETWORK_TYPE","EL_CALLING_VPN_TOP_GROUP_NUMBER","EL_CALLING_VPN_GROUP_NUMBER","EL_CALLING_VPN_SHORT_NUMBERs",
    "EL_CALLED_VPN_TOP_GROUP_NUMBER","EL_CALLED_VPN_GROUP_NUMBER","EL_CALLED_VPN_SHORT_NUMBER","EL_LAST_EFFECT_OFFERING","EL_ALTERNATE_ID",
    "EL_HOME_ZONE_ID","EL_USER_STATE","EL_PAY_DEFAULT_ACCT_ID","EL_TAX1","EL_TAX2",
    "EL_USER_GROUP_ID","EL_BUSINESS_TYPE","EL_SUBSCRIBER_KEY","EL_ACCOUNT_KEY","EL_DISCOUNT_OF_LAST_EFF_PROD",
    # Phase 4 aggregated additionalBalanceInfo fields (91-110)
    "EL_ADDITIONALBALANCEINFO_CHARGINGSERVICENAME","EL_ADDITIONALBALANCEINFO_USAGETYPE","EL_ADDITIONALBALANCEINFO_USEDAS",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETNAME","EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETUNITTYPE","EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETKINDOFUNIT",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEBEFORE","EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEAFTER","EL_ADDITIONALBALANCEINFO_BUCKETINFO_CARRYOVERBUCKET",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETCOMMITEDUNITS","EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETRESERVEDUNITS","EL_ADDITIONALBALANCEINFO_BUCKETINFO_RATEID",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_PRIMARYCOSTCOMMITTED","EL_ADDITIONALBALANCEINFO_BUCKETINFO_SECONDARYCOSTCOMMITTED","EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXATIONID",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXRATEAPPLIED","EL_ADDITIONALBALANCEINFO_BUCKETINFO_COMMITTEDTAXAMOUNT","EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALTAXAMOUNT",
    "EL_ADDITIONALBALANCEINFO_BUCKETINFO_TARIFFID","EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALUNITSCHARGED",
    # Phase 5 (111-114)
    "EL_UNLTD_BUNDLE_NAME","EL_UNLTD_ROUNDED_UNITS_CHARGED","EL_UNLTD_BUNDLE_UNIT_TYPE","EL_ORIG_LOCATION",
]


def enforce_canonical(mapped: Dict[str, Any]) -> Dict[str, Any]:
    """Return a dict with exactly the canonical 114 fields in order.

    Numeric defaults are applied only to explicitly known numeric fields.
    """
    out = {}
    # build explicit numeric fields set
    numeric_fields = {"EL_DEBIT_AMOUNT","EL_ON_NET_INDICATOR","EL_TAX1","EL_TAX2","EL_UNLTD_ROUNDED_UNITS_CHARGED"}
    for i in range(1, 6):
        numeric_fields.add(f"EL_CUR_BALANCE{i}")
        numeric_fields.add(f"EL_CHG_BALANCE{i}")
        numeric_fields.add(f"EL_BUCKET_CUR_BALANCE{i}")
        numeric_fields.add(f"EL_BUCKET_CHG_BALANCE{i}")
    # additionalBalance numeric fields (aggregated comma lists are strings, but keep numeric placeholders)
    numeric_fields.update({
        "EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEBEFORE",
        "EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEAFTER",
        "EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETCOMMITEDUNITS",
        "EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETRESERVEDUNITS",
        "EL_ADDITIONALBALANCEINFO_BUCKETINFO_PRIMARYCOSTCOMMITTED",
        "EL_ADDITIONALBALANCEINFO_BUCKETINFO_SECONDARYCOSTCOMMITTED",
        "EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXRATEAPPLIED",
        "EL_ADDITIONALBALANCEINFO_BUCKETINFO_COMMITTEDTAXAMOUNT",
        "EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALTAXAMOUNT",
        "EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALUNITSCHARGED",
    })

    for key in CANONICAL_FIELDS:
        if key in mapped and mapped[key] is not None and mapped[key] != "":
            out[key] = mapped[key]
        else:
            out[key] = 0.0 if key in numeric_fields else ""
    return out
