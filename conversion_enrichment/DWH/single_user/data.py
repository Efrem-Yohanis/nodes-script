# ...existing code...
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
import binascii
import re

INPUT = Path("file5.json")
OUTPUT = INPUT.with_name("mapped_table8.json")

def safe_get(d, *keys):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
        if cur is None:
            return None
    return cur

def to_decimal(v):
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        return None

def fmt_decimal(d):
    if d is None:
        return ""
    try:
        return format(d.quantize(Decimal("0.00000")), 'f')
    except Exception:
        return str(d)

def last_n_chars(s, n):
    if not s:
        return ""
    s = s.strip()
    return s[-n:] if len(s) >= n else s

def decode_location_hex_field(hexstr):
    if not hexstr:
        return ""
    s = last_n_chars(hexstr, 18)
    if len(s) < 18:
        # fallback to 14-char rule
        s14 = last_n_chars(hexstr, 14)
        if not s14:
            return ""
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

def collect_subscription_blocks(mscc):
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

def extract_noCharge_values(subs):
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

def extract_account_slots(subs, max_slots=5):
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
                    slots.append({
                        "accountID": acc.get("accountID", "") or "",
                        "accountType": acc.get("accountType", "") or "",
                        "accountBalanceAfter": acc.get("accountBalanceAfter", "") or "",
                        "accountBalanceBefore": acc.get("accountBalanceBefore", "") or "",
                        "accountBalanceCommitted": acc.get("accountBalanceCommitted", acc.get("accountBalanceCommittedBR", "")) or "",
                        "secondaryCostCommitted": acc.get("secondaryCostCommitted", "") or "",
                        "rateId": acc.get("rateId", "") or "",
                        "committedTaxAmount": acc.get("committedTaxAmount", "") or ""
                    })
                    break
            if len(slots) >= max_slots:
                break
    while len(slots) < max_slots:
        slots.append({
            "accountID": "",
            "accountType": "",
            "accountBalanceAfter": "",
            "accountBalanceBefore": "",
            "accountBalanceCommitted": "",
            "secondaryCostCommitted": "",
            "rateId": "",
            "committedTaxAmount": ""
        })
    return slots

def extract_bucket_slots(subs, max_slots=5):
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
            joined_rate_ids = ",".join(e["rateId"] for e in bucket_entries if e["rateId"])
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

def extract_subscription_ids(rec):
    msisdn = ""
    imsi = ""
    for ext in rec.get("recordExtensions", []) or []:
        if ext.get("recordProperty") == "listOfSubscriptionID":
            for sub in ext.get("recordSubExtensions", []) or []:
                if sub.get("recordProperty") != "subscriptionId":
                    continue
                re = sub.get("recordElements", {}) or {}
                sid = safe_get(re, "subscriptionId", "subscriptionIdData") or re.get("subscriptionIdData", "") or ""
                stype = safe_get(re, "subscriptionId", "subscriptionIdType") or re.get("subscriptionIdType", "")
                stype = str(stype)
                if stype == "0" and not msisdn:
                    msisdn = sid
                if stype == "1" and not imsi:
                    imsi = sid
    return msisdn, imsi

def extract_bundle_list(subs):
    bundles = []
    for sub in subs:
        bn = safe_get(sub, "recordElements", "bundleName") or ""
        if bn:
            bundles.append(bn)
    return bundles

def extract_alternate_ids(subs):
    vals = []
    for sub in subs:
        alt = safe_get(sub, "recordElements", "alternateId")
        if alt:
            vals.append(str(alt))
    return vals

def imei_from_user_equipment_value(hex_or_str):
    if not hex_or_str:
        return ""
    # try decode hex to ascii
    decoded = None
    try:
        b = binascii.unhexlify(hex_or_str)
        decoded = b.decode('ascii', errors='ignore')
        if not decoded:
            decoded = None
    except Exception:
        decoded = None
    src = decoded if decoded else hex_or_str
    # take even positions (1-based) => characters at indexes 1,3,5,... (0-based idx 1,3,...)
    even_chars = "".join(ch for i, ch in enumerate(src) if (i + 1) % 2 == 0)
    # keep only digits
    digits = re.sub(r'\D', '', even_chars)
    # ensure 16 digits: truncate or pad with zeros on right
    if len(digits) >= 16:
        return digits[:16]
    return digits.ljust(16, "0")

def map_table8(raw):
    rec = safe_get(raw, "record1", "payload", "genericRecord")
    if not rec:
        return {}
    elems = rec.get("recordElements", {}) or {}
    result = {}

    result["EL_CDR_ID"] = elems.get("sessionId", "") or ""
    # find first mscc
    mscc = None
    for ext in rec.get("recordExtensions", []) or []:
        if ext.get("recordProperty") == "listOfMscc":
            for sub in ext.get("recordSubExtensions", []) or []:
                if sub.get("recordProperty") == "mscc":
                    mscc = sub
                    break
            break
    mscc_e = (mscc.get("recordElements", {}) or {}) if mscc else {}

    result["EL_CDR_SUB_ID"] = mscc_e.get("localSequenceNumber", "") or ""
    result["EL_SRC_CDR_ID"] = ""
    result["EL_CUST_LOCAL_START_DATE"] = elems.get("generationTimestamp", "") or ""
    result["EL_RATE_USAGE"] = mscc_e.get("totalVolumeConsumed", mscc_e.get("timeUsage", "")) or ""
    # additional flux fields
    result["EL_TOTAL_FLUX"] = mscc_e.get("totalVolumeConsumed", "") or ""
    result["EL_UP_FLUX"] = mscc_e.get("uplinkVolumeConsumed", "") or ""
    result["EL_DOWN_FLUX"] = mscc_e.get("downlinkVolumeConsumed", "") or ""
    result["EL_ELAPSE_DURATION"] = elems.get("duration", "") or ""

    # EL_DEBIT_AMOUNT (per priority)
    debit_val = None
    if mscc:
        subs = collect_subscription_blocks(mscc)
        for sub in subs:
            for charge in sub.get("recordSubExtensions", []) or []:
                if charge.get("recordProperty") != "chargingServiceInfo":
                    continue
                for csub in charge.get("recordSubExtensions", []) or []:
                    if csub.get("recordProperty") == "accountInfo":
                        acc = csub.get("recordElements", {}) or {}
                        if acc.get("accountBalanceCommittedBR") not in (None, ""):
                            debit_val = to_decimal(acc.get("accountBalanceCommittedBR"))
                        elif acc.get("accountBalanceCommitted") not in (None, ""):
                            debit_val = to_decimal(acc.get("accountBalanceCommitted"))
                        else:
                            before = to_decimal(acc.get("accountBalanceBefore"))
                            after = to_decimal(acc.get("accountBalanceAfter"))
                            if before is not None and after is not None:
                                debit_val = before - after
                        break
                if debit_val is not None:
                    break
            if debit_val is not None:
                break
    result["EL_DEBIT_AMOUNT"] = fmt_decimal(debit_val)

    # Free units
    result["EL_FREE_UNIT_AMOUNT_OF_DURATION"] = ""
    free_flux_vals = []
    if mscc:
        free_flux_vals = extract_noCharge_values(collect_subscription_blocks(mscc))
    result["EL_FREE_UNIT_AMOUNT_OF_FLUX"] = ",".join(free_flux_vals) if free_flux_vals else ""

    # Account slots 1..5
    acct_slots = extract_account_slots(collect_subscription_blocks(mscc), max_slots=5)
    for i, slot in enumerate(acct_slots, start=1):
        result[f"EL_ACCT_BALANCE_ID{i}"] = slot["accountID"]
        result[f"EL_BALANCE_TYPE{i}"] = slot["accountType"]
        result[f"EL_CUR_BALANCE{i}"] = slot["accountBalanceAfter"]
        before = to_decimal(slot["accountBalanceBefore"])
        after = to_decimal(slot["accountBalanceAfter"])
        committed = to_decimal(slot["accountBalanceCommitted"])
        secondary = to_decimal(slot["secondaryCostCommitted"])
        chg = None
        if before is not None and after is not None:
            diff = before - after
            if diff < 0:
                add = Decimal(0)
                if committed is not None:
                    add += committed
                if secondary is not None:
                    add += secondary
                chg = add
            else:
                chg = diff
        result[f"EL_CHG_BALANCE{i}"] = fmt_decimal(chg)
        result[f"EL_RATE_ID{i}"] = slot.get("rateId", "") or ""
    # TAX1 from first account slot if present
    result["EL_TAX1"] = acct_slots[0].get("committedTaxAmount", "") or ""

    # Bucket slots 1..5 and TAX2 from first bucket
    bucket_slots = extract_bucket_slots(collect_subscription_blocks(mscc), max_slots=5)
    for i, bslot in enumerate(bucket_slots, start=1):
        result[f"EL_BUCKET_BALANCE_ID{i}"] = bslot["bucket_balance_id"]
        result[f"EL_BUCKET_BALANCE_TYPE{i}"] = bslot["bucket_unit_type"]
        result[f"EL_BUCKET_CUR_BALANCE{i}"] = bslot["bucket_cur_balance"]
        result[f"EL_BUCKET_CHG_BALANCE{i}"] = bslot["bucket_chg_balance"]
        result[f"EL_BUCKET_RATE_ID{i}"] = bslot["bucket_rate_id"]
    result["EL_TAX2"] = bucket_slots[0].get("committedTaxAmount", "") or ""

    # Subscription ids
    msisdn, imsi = extract_subscription_ids(rec)
    result["EL_CALLING_PARTY_NUMBER"] = msisdn
    result["EL_CALLING_PARTY_IMSI"] = imsi

    # APN, URL
    result["EL_APN"] = elems.get("accessPointName", "") or ""
    result["EL_URL"] = ""

    # IMEI from userEquipmentValue
    result["EL_IMEI"] = imei_from_user_equipment_value(elems.get("userEquipmentValue", "") or "")

    result["EL_BEARER_PROTOCOL_TYPE"] = ""
    # main offering id: comma separated bundleName from all subscriptionInfo blocks
    result["EL_MAIN_OFFERING_ID"] = ",".join(extract_bundle_list(collect_subscription_blocks(mscc))) or ""
    # Pay type: use EL_PRE_POST if present else empty
    result["EL_PAY_TYPE"] = elems.get("EL_PRE_POST", "") or ""
    # charging type: first chargingServiceType found
    charging_type = ""
    if mscc:
        for sub in collect_subscription_blocks(mscc):
            for charge in sub.get("recordSubExtensions", []) or []:
                if charge.get("recordProperty") == "chargingServiceInfo":
                    charging_type = safe_get(charge, "recordElements", "chargingServiceType") or charge.get("recordElements", {}).get("chargingServiceType", "")
                    if charging_type:
                        break
            if charging_type:
                break
    result["EL_CHARGING_TYPE"] = charging_type or ""
    result["EL_ROAM_STATE"] = elems.get("roamingIndicator", "") or ""
    result["EL_CALLING_VPN_TOP_GROUP_NUMBER"] = ""
    result["EL_CALLING_VPN_GROUP_NUMBER"] = ""
    result["EL_START_TIME_OF_BILL_CYCLE"] = elems.get("recordOpeningTime", "") or ""
    result["EL_LAST_EFFECT_OFFERING"] = ""
    result["EL_RATING_GROUP"] = mscc_e.get("ratingGroup", "") or ""
    result["EL_USER_STATE"] = elems.get("deviceState", "") or ""
    result["EL_RAT_TYPE"] = elems.get("rATType", "") or ""
    result["EL_CHARGE_PARTY_INDICATOR"] = ""
    result["EL_COUNTRY_NAME"] = ""
    result["EL_PAY_DEFAULT_ACCT_ID"] = ""
    result["EL_LOCATION"] = ""
    # location: use userLocationInformation decoding per RAT
    if str(elems.get("rATType")) == "6":
        result["EL_LOCATION"] = decode_location_hex_field(elems.get("userLocationInformation", "") or "")
    else:
        s14 = last_n_chars(elems.get("userLocationInformation", "") or "", 14)
        if s14:
            result["EL_LOCATION"] = f"{s14[-14:-8]}-{s14[-8:-4]}-{s14[-4:]}"
    # alternate ids across subscriptionInfo blocks
    result["EL_ALTERNATE_ID"] = "~".join(extract_alternate_ids(collect_subscription_blocks(mscc))) or ""
    result["EL_BUSINESS_TYPE"] = ""
    result["EL_SUBSCRIBER_KEY"] = ""
    result["EL_ACCOUNT_KEY"] = ""

    return result

if __name__ == "__main__":
    raw = json.loads(INPUT.read_text(encoding="utf-8"))
    mapped = map_table8(raw)
    OUTPUT.write_text(json.dumps(mapped, indent=4), encoding="utf-8")
    print(json.dumps(mapped, indent=4))
# ...existing code...