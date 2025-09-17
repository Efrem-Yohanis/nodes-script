import sys
import json
import argparse
from datetime import datetime
import re

def safe_get(d, *keys):
    cur = d
    for k in keys:
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(k)
        else:
            return None
    return cur

def find_extension(record_extensions, prop):
    if not record_extensions:
        return None
    for ext in record_extensions:
        if ext.get("recordProperty") == prop:
            return ext
    return None

def parse_timestamp_ts(ts):
    """Parse common timestamp formats like '15/08/2025 15:26:53+03:00' or '15/08/2025 12:26:53'."""
    if not ts:
        return ""
    s = ts.strip()
    # strip trailing timezone markers like +03:00, -03:00 or Z
    s = re.sub(r'([+-]\d{2}:\d{2}|Z)$', '', s).strip()
    # try with seconds then without seconds
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
    # fallback: return original input if parsing fails
    return ts

def to_float(val, default=None):
    try:
        return float(val)
    except Exception:
        return default

def get_subscription_ids(record):
    ext = find_extension(record.get("recordExtensions", []), "listOfSubscriptionID")
    if not ext:
        return []
    out = []
    for sub in ext.get("recordSubExtensions", []):
        if sub.get("recordProperty") == "subscriptionId":
            elems = sub.get("recordElements", {}) or {}
            out.append(elems)
    return out

def get_listof_mscc(record):
    ext = find_extension(record.get("recordExtensions", []), "listOfMscc")
    if not ext:
        return []
    out = []
    for mscc in ext.get("recordSubExtensions", []):
        if mscc.get("recordProperty") == "mscc":
            out.append(mscc)
    return out

def format_imei_from_user_equipment(value):
    if not value:
        return ""
    chars = []
    for i in range(0, min(len(value), 31), 2):
        chars.append(value[i])
    imei = "".join(chars)
    if len(imei) < 16:
        imei = imei.ljust(16, "0")
    return imei[:16]

def decode_location_13(user_loc):
    if not user_loc:
        return ""
    s = user_loc[-13:]
    if len(s) < 13:
        return user_loc
    part1 = s[0:5]
    part2 = s[5:9]
    part3 = s[9:13]
    return f"{part1}-{part2}-{part3}"

def build_crm_record_voice(input_data, input_filename=None):
    gr = safe_get(input_data, "payload", "genericRecord") or {}
    rec_elems = gr.get("recordElements", {}) or {}
    rec_extensions = gr.get("recordExtensions", []) or []

    mscc_list = get_listof_mscc(gr)
    sub_ids = get_subscription_ids(gr)

    # Build subscription_blocks from first mscc (deviceInfo -> subscriptionInfo list)
    subscription_blocks = []
    if mscc_list:
        mscc0 = mscc_list[0]
        device_info = None
        for s in mscc0.get("recordSubExtensions", []):
            if s.get("recordProperty") == "deviceInfo":
                device_info = s
                break
        if device_info:
            for ssub in device_info.get("recordSubExtensions", []):
                if ssub.get("recordProperty") == "subscriptionInfo":
                    subscription_blocks.append(ssub)

    # Helpers to find subscriptionInfo blocks matching criteria
    def find_first_accountinfo_only():
        for sb in subscription_blocks:
            for item in sb.get("recordSubExtensions", []):
                if item.get("recordProperty") == "chargingServiceInfo":
                    # check if within this chargingServiceInfo there is accountInfo and no bucketInfo
                    has_account = False
                    has_bucket = False
                    for csis in item.get("recordSubExtensions", []):
                        if csis.get("recordProperty") == "accountInfo":
                            has_account = True
                        if csis.get("recordProperty") == "bucketInfo":
                            has_bucket = True
                    if has_account and not has_bucket:
                        # return the accountInfo elements and also subscriptionInfo elements for bundleName
                        # find accountInfo element block
                        for csis in item.get("recordSubExtensions", []):
                            if csis.get("recordProperty") == "accountInfo":
                                return csis.get("recordElements", {}) or {}, sb.get("recordElements", {}) or {}
        return {}, {}

    def find_bucket_blocks():
        # return list of tuples (subscription_bundleName, bucket_elems, optional_additional_balance_info)
        out = []
        for sb in subscription_blocks:
            bundle_name = (sb.get("recordElements", {}) or {}).get("bundleName")
            for item in sb.get("recordSubExtensions", []):
                if item.get("recordProperty") == "chargingServiceInfo":
                    for csis in item.get("recordSubExtensions", []):
                        if csis.get("recordProperty") == "bucketInfo":
                            out.append((bundle_name, csis.get("recordElements", {}) or {}))
        return out

    account_info, account_sb_elems = find_first_accountinfo_only()
    bucket_blocks = find_bucket_blocks()

    # Helper: get subrecord event type (from mscc recordElements.recordEventType)
    subrecord_event_type = None
    if mscc_list:
        try:
            subrecord_event_type = mscc_list[0].get("recordElements", {}).get("recordEventType")
        except Exception:
            subrecord_event_type = None

    # EL_ACCOUNT_ID decision: use subscriptionId where type==0
    def pick_account_id():
        chosen = None
        for s in sub_ids:
            if s.get("subscriptionIdType") == "0" or str(s.get("subscriptionIdType")) == "0":
                chosen = s.get("subscriptionIdData")
                break
        if not chosen:
            return ""
        roaming = str(rec_elems.get("roamingIndicator", "")).upper().startswith("ROAM")
        cond = (roaming and (subrecord_event_type == "MTC")) or (subrecord_event_type == "FWD")
        if cond:
            val = chosen
            if not val:
                return ""
            if not val.startswith("251") and len(val) < 10:
                # remove leading 0 if present then prepend 251
                if val.startswith("0"):
                    val = val[1:]
                val = "251" + val
            return val
        else:
            return chosen

    def transform_calling_party(cp):
        if not cp:
            return cp
        roaming = str(rec_elems.get("roamingIndicator", "")).upper().startswith("ROAM")
        cond = (roaming and (subrecord_event_type == "MTC")) or (subrecord_event_type == "FWD")
        if cond:
            val = cp
            if not val.startswith("251") and len(val) < 10:
                if val.startswith("0"):
                    val = val[1:]
                val = "251" + val
            return val
        else:
            return cp

    out = {}
    # 1 EL_LMS
    out["EL_LMS"] = 1 if str(rec_elems.get("EL_SUCCESS", "")) == "1" else 0
    # 2 EL_GENERATION_TIMESTAMP: prefer callAnswerTime -> recordOpeningTime -> generationTimestamp
    gen_ts = rec_elems.get("callAnswerTime") or rec_elems.get("recordOpeningTime") or rec_elems.get("generationTimestamp")
    out["EL_GENERATION_TIMESTAMP"] = parse_timestamp_ts(gen_ts)
    # 3 EL_EVENT_LABEL - from mscc.recordEventType
    out["EL_EVENT_LABEL"] = subrecord_event_type or ""
    # 4 EL_ACCOUNT_ID
    out["EL_ACCOUNT_ID"] = pick_account_id() or ""
    # 5 EL_DIALLED_DIGITS
    # if condition: use callingPartyAddress transformed, else use calledPartyAddress
    calling = rec_elems.get("callingPartyAddress")
    called = rec_elems.get("calledPartyAddress")
    roaming = str(rec_elems.get("roamingIndicator", "")).upper().startswith("ROAM")
    cond = (roaming and (subrecord_event_type == "MTC")) or (subrecord_event_type == "FWD")
    if cond:
        out["EL_DIALLED_DIGITS"] = transform_calling_party(calling) or calling or called or ""
    else:
        out["EL_DIALLED_DIGITS"] = called or ""
    # 6 EL_CALL_DURATION from mscc.totalTimeConsumed
    try:
        ms0 = mscc_list[0] if mscc_list else {}
        out["EL_CALL_DURATION"] = ms0.get("recordElements", {}).get("totalTimeConsumed")
    except Exception:
        out["EL_CALL_DURATION"] = None
    # 7 EL_CALL_COST: from first subscriptionInfo block where accountInfo exists and not bucketInfo
    call_cost = None
    if account_info:
        br = account_info.get("accountBalanceCommittedBR")
        committed = account_info.get("accountBalanceCommitted")
        before = account_info.get("accountBalanceBefore")
        after = account_info.get("accountBalanceAfter")
        if br:
            call_cost = to_float(br, None)
        elif committed:
            call_cost = to_float(committed, None)
        elif before is not None and after is not None:
            try:
                call_cost = to_float(float(before) - float(after), None)
            except Exception:
                call_cost = None
    out["EL_CALL_COST"] = call_cost if call_cost is not None else None
    # 8 EL_LOCATION_INFORMATION: last 13 chars -> 5-4-4 split
    out["EL_LOCATION_INFORMATION"] = decode_location_13(rec_elems.get("userLocationInformation") or "")
    # 9 EL_TARIFF_PLAN: bundleName from first subscriptionInfo with accountInfo only and ratingGroup appended with -
    rating_group = None
    if mscc_list:
        rating_group = mscc_list[0].get("recordElements", {}).get("ratingGroup")
    bundle = (account_sb_elems.get("bundleName") if account_sb_elems else None) or ""
    if bundle or rating_group:
        if bundle:
            out["EL_TARIFF_PLAN"] = f"{bundle}-{rating_group}" if rating_group else bundle
        else:
            out["EL_TARIFF_PLAN"] = rating_group or ""
    else:
        out["EL_TARIFF_PLAN"] = ""
    # 10 EL_CREDIT_EXPIRYDATE
    out["EL_CREDIT_EXPIRYDATE"] = None
    # 11 EL_SCP_NUMBER meHostName
    out["EL_SCP_NUMBER"] = rec_elems.get("meHostName") or ""
    # 12 EL_POST_EVENT_PRIMARY_BALANCE from account_info.accountBalanceAfter
    out["EL_POST_EVENT_PRIMARY_BALANCE"] = account_info.get("accountBalanceAfter") if account_info else None
    # 13 EL_CURRENCY_IDENTIFIER fixed "Cent"
    out["EL_CURRENCY_IDENTIFIER"] = "Cent"
    # 14 EL_EVENT_RESULT
    rc = rec_elems.get("resultCode")
    out["EL_EVENT_RESULT"] = 1 if str(rc) == "2001" else rc
    # 15 EL_FIRSTCALL_FLAG fixed
    out["EL_FIRSTCALL_FLAG"] = "false"
    # 16 EL_CALL_START_TIME generationTimestamp converted
    out["EL_CALL_START_TIME"] = parse_timestamp_ts(rec_elems.get("generationTimestamp"))
    # 17 EL_ROAMING_INDICATOR
    out["EL_ROAMING_INDICATOR"] = 1 if str(rec_elems.get("roamingIndicator", "")).upper().startswith("ROAM") else 0
    # 18 EL_EVENT_SIM_STATECODE default Active
    out["EL_EVENT_SIM_STATECODE"] = "Active"
    # 19 EL_CALL_DIRECTION mediaName
    out["EL_CALL_DIRECTION"] = rec_elems.get("mediaName") or ""
    # 20 EL_SEQUENCE_NUMBER sessionSequenceNumber
    out["EL_SEQUENCE_NUMBER"] = rec_elems.get("sessionSequenceNumber") or ""
    # fields 21-24 null
    out["EL_REDIRECTING_PARTY_ADDRESS"] = None
    out["EL_CIRCLE_ID"] = None
    out["EL_ORIGINATING_ZONE_CODE"] = None
    out["EL_DESTINATION_ZONE_CODE"] = None
    # 25 EL_DISCOUNT_ID - pipe separated list built from bucket blocks; concatenate bundleName-bucketName for each bucket occurrence
    discount_parts = []
    for b in bucket_blocks:
        bundle_name, bucket_elems = b
        bucket_name = bucket_elems.get("bucketName")
        if bucket_name:
            if bundle_name:
                discount_parts.append(f"{bundle_name}-{bucket_name}")
            else:
                discount_parts.append(bucket_name)
    out["EL_DISCOUNT_ID"] = "|".join(discount_parts) if discount_parts else ""
    # 26-40 bucket precall/postcall mapping (1..5)
    for i in range(1, 6):
        out[f"EL_BUCKET_VALUE{i}_PRECALL"] = None
        out[f"EL_BUCKET_VALUE{i}_POSTCALL"] = None
    idx = 1
    for b in bucket_blocks:
        _, be = b
        if idx <= 5:
            out[f"EL_BUCKET_VALUE{idx}_PRECALL"] = be.get("bucketBalanceBefore")
            out[f"EL_BUCKET_VALUE{idx}_POSTCALL"] = be.get("bucketBalanceAfter")
            idx += 1
    # 31-35 & 45 special handling for bucket 10 values (additionalBalanceInfo)
    # find additionalBalanceInfo.bucketInfo inside subscription blocks
    add_pre10 = None
    add_post10 = None
    add_committed10 = None
    for sb in subscription_blocks:
        for item in sb.get("recordSubExtensions", []):
            if item.get("recordProperty") == "chargingServiceInfo":
                for csis in item.get("recordSubExtensions", []):
                    if csis.get("recordProperty") == "additionalBalanceInfo":
                        for add in csis.get("recordSubExtensions", []):
                            if add.get("recordProperty") == "bucketInfo":
                                elems = add.get("recordElements", {}) or {}
                                if elems.get("bucketBalanceBefore") is not None:
                                    add_pre10 = round(to_float(elems.get("bucketBalanceBefore"), 0)/100.0, 2)
                                if elems.get("bucketBalanceAfter") is not None:
                                    add_post10 = round(to_float(elems.get("bucketBalanceAfter"), 0)/100.0, 2)
                                if elems.get("bucketCommitedUnits") is not None:
                                    add_committed10 = round(to_float(elems.get("bucketCommitedUnits"), 0)/100.0, 2)
    out["EL_BUCKET_VALUE10_PRECALL"] = add_pre10
    out["EL_BUCKET_VALUE10_POSTCALL"] = add_post10
    # 46-48 etc. some null/defaults
    out["EL_MULTIPLE_SEQUENCE_NUMBER"] = None
    out["EL_QOS_RANGE_LABEL"] = None
    # 48 EL_TOTAL_USED_FREE_SECONDS -> sum of bucketCommitedUnits across bucketInfo occurrences
    total_committed = 0
    found_committed = False
    for b in bucket_blocks:
        be = b[1]
        val = be.get("bucketCommitedUnits")
        if val is not None:
            v = to_float(val, None)
            if v is not None:
                total_committed += v
                found_committed = True
    if found_committed:
        out["EL_TOTAL_USED_FREE_SECONDS"] = total_committed
    else:
        out["EL_TOTAL_USED_FREE_SECONDS"] = None
    # 49 EL_CALL_VOLUME null (per table)
    out["EL_CALL_VOLUME"] = None
    # 50 gGSNAddress direct
    out["EL_GGSN_ADDRESS"] = rec_elems.get("gGSNAddress") or ""
    # 52 bearer capability
    out["EL_BEARER_CAPABILITY"] = rec_elems.get("mediaName") or ""
    # 56 charging id
    out["EL_CHARGING_ID"] = rec_elems.get("sessionId") or ""
    # many nulls/defaults
    out["EL_RECHARGE_AMOUNT"] = None
    out["EL_NOMINAL_AMOUNT"] = None
    out["EL_VALIDITY"] = None
    out["EL_MERCHANT_ID"] = None
    out["EL_GRACE2_DATE"] = None
    out["EL_GRACE1_DATE"] = None
    out["EL_RECHARGE_CODE"] = None
    # 64 applied discount id maps to EL_DISCOUNT_ID
    out["EL_APPLIED_DISCOUNTID"] = out["EL_DISCOUNT_ID"]
    out["EL_PREEVENT_SUBSCRIBER_STATUS"] = "Active"
    out["EL_SUBSCRIPTION_CHARGE"] = None
    out["EL_PROMOTIONAL_TARIFF_PLAN"] = None
    # 86 pre event primary balance from first accountInfo-only block
    out["EL_PRE_EVENT_PRIMARY_BALANCE"] = account_info.get("accountBalanceBefore") if account_info else None
    out["EL_BAND_LABEL_AMA_CODE"] = "diameter"
    # 94-99 bucket usage types for first 5 bucket blocks
    for i in range(1, 6):
        out[f"EL_BUCKETVALUE{i}_USAGETYPE"] = None
    idx = 1
    for b in bucket_blocks:
        be = b[1]
        if idx <= 5:
            out[f"EL_BUCKETVALUE{idx}_USAGETYPE"] = be.get("bucketUnitType")
            idx += 1
    # 104 EL_VOICE_BUCKET_USAGE -> if bucketCommitedUnits present sum else if additionalBalanceInfo present map it (handled earlier)
    voice_usage = None
    if total_committed and total_committed != 0:
        voice_usage = total_committed
    elif add_committed10 is not None:
        voice_usage = add_committed10
    out["EL_VOICE_BUCKET_USAGE"] = voice_usage
    # 107 EL_IMSI subscriptionIdType == '1'
    out["EL_IMSI"] = None
    for s in sub_ids:
        if s.get("subscriptionIdType") == "1" or str(s.get("subscriptionIdType")) == "1":
            out["EL_IMSI"] = s.get("subscriptionIdData")
            break
    out["EL_OUTSTANDING_CHARGES"] = None
    out["EL_CDR_REFERENCE_NUMBER"] = rec_elems.get("sessionSequenceNumber")
    # 110 alternateId
    out["EL_ALTERNATE_ID"] = None
    for sb in subscription_blocks:
        elems = sb.get("recordElements", {}) or {}
        alt = elems.get("alternateId")
        if alt:
            out["EL_ALTERNATE_ID"] = alt
            break
    # 113 default tariff plan (bundleName from first subscriptionInfo where bucketInfo does not exist and accountInfo exists)
    out["EL_DEFAULT_TARIFF_PLAN_AMA_COSP_CODE"] = (account_sb_elems.get("bundleName") if account_sb_elems else "") or ""
    out["EL_PROTOCOL_TYPE"] = "diameter"
    out["EL_IMEI"] = format_imei_from_user_equipment(rec_elems.get("userEquipmentValue"))
    out["EL_PTP_COSP_AMA_CODE"] = out["EL_DEFAULT_TARIFF_PLAN_AMA_COSP_CODE"]
    out["EL_PROCESS_FILENAME"] = input_filename or ""
    # 205 processed timestamp uses callAnswerTime
    out["EL_PROCESSED_TIMESTAMP"] = parse_timestamp_ts(rec_elems.get("callAnswerTime"))
    out["EL_ISCONTENTCDR"] = "false"
    out["EL_CLASS_OF_SERVICE_CODE"] = "0"
    # 201 sGSNAddress
    out["EL_SERVING_SGSN_IP_ADDRESS"] = rec_elems.get("sGSNAddress") or ""
    out["EL_ENHANCED_ACCESS_TECHNOLOGY_TYPE"] = rec_elems.get("rATType")
    out["EL_TRANSACTION_ID"] = rec_elems.get("sessionId")
    # 177 MDN series last two digits of EL_ACCOUNT_ID
    try:
        aid = str(out.get("EL_ACCOUNT_ID") or "")
        out["EL_MDN_SERIES"] = aid[-2:] if len(aid) >= 2 else aid
    except Exception:
        out["EL_MDN_SERIES"] = None
    # 178 calling party address mapping similar to earlier transformation
    cp = rec_elems.get("callingPartyAddress")
    out["EL_CALLING_PARTY_ADDRESS"] = transform_calling_party(cp) if cp else cp
    # many remaining fields set to None or defaults
    # ... add any additional minimal defaults required by downstream systems ...
    return out

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="infile", required=True, help="input JSON file")
    parser.add_argument("--out", dest="outdir", required=False, help="output folder", default=None)
    args = parser.parse_args()
    infile = args.infile
    outdir = args.outdir

    with open(infile, "r", encoding="utf-8") as f:
        data = json.load(f)

    import os
    basename = os.path.basename(infile)
    out_rec = build_crm_record_voice(data, input_filename=basename)

    out_name = os.path.splitext(basename)[0] + "_voice_crm.json"
    out_path = os.path.join(outdir or os.path.dirname(infile), out_name)
    with open(out_path, "w", encoding="utf-8") as fo:
        json.dump(out_rec, fo, ensure_ascii=False, indent=2)
    print("Wrote:", out_path)

if __name__ == "__main__":
    main()