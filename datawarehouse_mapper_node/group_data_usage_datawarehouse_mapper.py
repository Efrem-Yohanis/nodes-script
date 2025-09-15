import json
import time
import shutil
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal, InvalidOperation

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("group_data_cdr_mapper")

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
    swapped_clean = swapped.replace('F', '')
    try:
        eci_int = int(eci_hex, 16)
    except Exception:
        eci_int = 0
    enb = str(eci_int % 256)
    cell = str(eci_int // 256)
    return f"{swapped_clean}-{tac_dec}-{enb}-{cell}"

# Collect listOfMscc extension

def find_list_of_mscc(rec: dict) -> Optional[dict]:
    for ext in rec.get("recordExtensions", []) or []:
        if ext.get("recordProperty") == "listOfMscc":
            return ext
    return None

# Collect group subscriptionInfo blocks according to precedence rules
def collect_group_subscription_blocks(rec: dict) -> List[dict]:
    list_of_mscc = find_list_of_mscc(rec)
    subs: List[dict] = []
    if not list_of_mscc:
        return subs
    # Prefer mscc.groupInfo when present
    mscc_blocks = [s for s in (list_of_mscc.get("recordSubExtensions", []) or []) if s.get("recordProperty") == "mscc"]
    for mscc in mscc_blocks:
        for child in (mscc.get("recordSubExtensions", []) or []):
            if child.get("recordProperty") == "groupInfo":
                # collect subscriptionInfo under this groupInfo
                for gsub in (child.get("recordSubExtensions", []) or []):
                    if gsub.get("recordProperty") == "subscriptionInfo":
                        subs.append(gsub)
        if subs:
            return subs
    # fallback: look for listOfMscc.groupInfo at top-level of listOfMscc
    for child in (list_of_mscc.get("recordSubExtensions", []) or []):
        if child.get("recordProperty") == "groupInfo":
            for gsub in (child.get("recordSubExtensions", []) or []):
                if gsub.get("recordProperty") == "subscriptionInfo":
                    subs.append(gsub)
    return subs

# Extract subscription IDs (msisdn/imsi)
def extract_subscription_ids(rec: dict) -> Tuple[str,str]:
    msisdn = ""
    imsi = ""
    for ext in rec.get("recordExtensions", []) or []:
        if ext.get("recordProperty") == "listOfSubscriptionID":
            for sub in ext.get("recordSubExtensions", []) or []:
                if sub.get("recordProperty") != "subscriptionId":
                    continue
                elems = sub.get("recordElements", {}) or {}
                dtype = str(elems.get("subscriptionIdType") or elems.get("subscriptionIDType") or "")
                data = elems.get("subscriptionIdData") or elems.get("subscriptionIDData") or ""
                if dtype == "0" and not msisdn:
                    msisdn = data
                if dtype == "1" and not imsi:
                    imsi = data
    return msisdn, imsi

# Extract account and bucket slots from collected subscription blocks
def extract_account_slots_from_subs(subs: List[dict], max_slots: int = 5) -> List[dict]:
    slots = []
    for sub in subs:
        if len(slots) >= max_slots:
            break
        acct_info = None
        for charge in (sub.get("recordSubExtensions", []) or []):
            if charge.get("recordProperty") != "chargingServiceInfo":
                continue
            for csub in (charge.get("recordSubExtensions", []) or []):
                if csub.get("recordProperty") == "accountInfo" and acct_info is None:
                    acct_info = csub.get("recordElements", {}) or {}
        if acct_info:
            slots.append({
                "accountID": acct_info.get("accountID") or "",
                "accountType": acct_info.get("accountType") or "",
                "accountBalanceAfter": acct_info.get("accountBalanceAfter") or "",
                "accountBalanceBefore": acct_info.get("accountBalanceBefore") or "",
                "accountBalanceCommitted": acct_info.get("accountBalanceCommitted") or acct_info.get("accountBalanceCommittedBR") or "",
                "secondaryCostCommitted": acct_info.get("secondaryCostCommitted") or "",
                "rateId": acct_info.get("rateId") or "",
                "committedTaxAmount": acct_info.get("committedTaxAmount") or "",
                "totalVolumeCharged": acct_info.get("totalVolumeCharged") or acct_info.get("totalUnitsCharged") or "",
                "roundedVolumeCharged": acct_info.get("roundedVolumeCharged") or acct_info.get("roundedVolumeCharged") or "",
            })
    while len(slots) < max_slots:
        slots.append({
            "accountID": "","accountType": "","accountBalanceAfter": "","accountBalanceBefore": "",
            "accountBalanceCommitted": "","secondaryCostCommitted": "","rateId": "","committedTaxAmount": "",
            "totalVolumeCharged": "","roundedVolumeCharged": ""
        })
    return slots

def extract_bucket_slots_from_subs(subs: List[dict], max_slots: int = 5) -> List[dict]:
    slots = []
    for sub in subs:
        if len(slots) >= max_slots:
            break
        bundle_name = safe_get(sub, ["recordElements","bundleName"]) or ""
        bucket_entries = []
        for charge in (sub.get("recordSubExtensions", []) or []):
            if charge.get("recordProperty") != "chargingServiceInfo":
                continue
            for csub in (charge.get("recordSubExtensions", []) or []):
                if csub.get("recordProperty") == "bucketInfo":
                    b = csub.get("recordElements", {}) or {}
                    bucket_entries.append({
                        "bucketName": b.get("bucketName") or "",
                        "bucketUnitType": b.get("bucketUnitType") or "",
                        "bucketBalanceAfter": b.get("bucketBalanceAfter") or "",
                        "bucketBalanceBefore": b.get("bucketBalanceBefore") or "",
                        "bucketCommitedUnits": b.get("bucketCommitedUnits") or "",
                        "rateId": b.get("rateId") or "",
                        "committedTaxAmount": b.get("committedTaxAmount") or "",
                    })
        if bucket_entries:
            joined_bucket_names = ",".join((f"{bundle_name}-{e['bucketName']}" if bundle_name else e['bucketName']) for e in bucket_entries)
            joined_unit_types = ",".join(e['bucketUnitType'] for e in bucket_entries if e.get('bucketUnitType'))
            joined_balance_after = ",".join(e['bucketBalanceAfter'] for e in bucket_entries)
            chg_list = []
            for e in bucket_entries:
                before = to_decimal(e['bucketBalanceBefore'])
                after = to_decimal(e['bucketBalanceAfter'])
                if before is not None and after is not None:
                    diff = before - after
                    if diff < 0:
                        chg_list.append(e.get('bucketCommitedUnits') or "")
                    else:
                        chg_list.append(str(diff))
                else:
                    chg_list.append("")
            joined_chg = ",".join(chg_list)
            joined_rate_ids = ",".join(e['rateId'] for e in bucket_entries if e.get('rateId'))
            committed_tax = ""
            for e in bucket_entries:
                if e.get('committedTaxAmount'):
                    committed_tax = e.get('committedTaxAmount')
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
            "bucket_balance_id": "","bucket_unit_type": "","bucket_cur_balance": "","bucket_chg_balance": "","bucket_rate_id": "","committedTaxAmount": ""
        })
    return slots

# Map function
def map_group_data(cdr_json: Dict[str, Any]) -> Dict[str, Any]:
    generic = safe_get(cdr_json, ["original","payload","genericRecord"]) or cdr_json
    elems = safe_get(generic, ["recordElements"]) or {}
    cbl = cdr_json.get("CBL_TAG") or {}

    out: Dict[str, Any] = {}
    out["EL_CDR_ID"] = elems.get("sessionId") or ""
    # EL_CDR_SUB_ID from mscc.localSequenceNumber
    mscc_local_seq = ""
    list_of_mscc = find_list_of_mscc(generic)
    if list_of_mscc:
        for sub in (list_of_mscc.get("recordSubExtensions", []) or []):
            if sub.get("recordProperty") == "mscc":
                mscc_local_seq = (safe_get(sub, ["recordElements","localSequenceNumber"]) or "")
                break
    out["EL_CDR_SUB_ID"] = mscc_local_seq
    out["EL_SRC_CDR_ID"] = ""
    out["EL_CUST_LOCAL_START_DATE"] = elems.get("generationTimestamp") or ""
    # rate usage direct
    rate_usage = ""
    if list_of_mscc:
        for sub in (list_of_mscc.get("recordSubExtensions", []) or []):
            if sub.get("recordProperty") == "mscc":
                rate_usage = safe_get(sub, ["recordElements","totalVolumeConsumed"]) or safe_get(sub, ["recordElements","timeUsage"]) or ""
                break
    out["EL_RATE_USAGE"] = rate_usage

    # collect group subscription blocks (mscc.groupInfo preferred)
    group_subs = collect_group_subscription_blocks(generic)

    # EL_DEBIT_AMOUNT using groupInfo precedence
    debit_val = None
    for sub in group_subs:
        for charge in (sub.get("recordSubExtensions", []) or []):
            if charge.get("recordProperty") != "chargingServiceInfo":
                continue
            for csub in (charge.get("recordSubExtensions", []) or []):
                if csub.get("recordProperty") == "accountInfo":
                    acc = csub.get("recordElements", {}) or {}
                    dv = to_decimal(acc.get("accountBalanceCommitted")) or to_decimal(acc.get("accountBalanceCommittedBR"))
                    if dv is not None:
                        debit_val = dv
                        break
                    before = to_decimal(acc.get("accountBalanceBefore"))
                    after = to_decimal(acc.get("accountBalanceAfter"))
                    if before is not None and after is not None:
                        debit_val = before - after
                        break
            if debit_val is not None:
                break
        if debit_val is not None:
            break
    out["EL_DEBIT_AMOUNT"] = fmt_decimal_to_float(debit_val) if debit_val is not None else 0.0

    out["EL_FREE_UNIT_AMOUNT_OF_DURATION"] = ""

    # free flux from group noCharge
    free_vals = []
    for sub in group_subs:
        for charge in (sub.get("recordSubExtensions", []) or []):
            if charge.get("recordProperty") != "chargingServiceInfo":
                continue
            for csub in (charge.get("recordSubExtensions", []) or []):
                if csub.get("recordProperty") == "noCharge":
                    nc = csub.get("recordElements", {}) or {}
                    v = nc.get("noChargeCommittedUnits")
                    if v not in (None, ""):
                        free_vals.append(str(v))
    out["EL_FREE_UNIT_AMOUNT_OF_FLUX"] = ",".join(free_vals) if free_vals else ""

    # account and bucket slots
    acct_slots = extract_account_slots_from_subs(group_subs, max_slots=5)
    for i, slot in enumerate(acct_slots, start=1):
        out[f"EL_ACCT_BALANCE_ID{i}"] = slot.get("accountID") or ""
        out[f"EL_BALANCE_TYPE{i}"] = slot.get("accountType") or ""
        out[f"EL_CUR_BALANCE{i}"] = slot.get("accountBalanceAfter") or ""
        before = to_decimal(slot.get("accountBalanceBefore"))
        after = to_decimal(slot.get("accountBalanceAfter"))
        committed = to_decimal(slot.get("accountBalanceCommitted"))
        secondary = to_decimal(slot.get("secondaryCostCommitted"))
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
        out[f"EL_CHG_BALANCE{i}"] = fmt_decimal_to_float(chg) if chg is not None else 0.0
        out[f"EL_RATE_ID{i}"] = slot.get("rateId") or ""

    bucket_slots = extract_bucket_slots_from_subs(group_subs, max_slots=5)
    for i, b in enumerate(bucket_slots, start=1):
        out[f"EL_BUCKET_BALANCE_ID{i}"] = b.get("bucket_balance_id") or ""
        out[f"EL_BUCKET_BALANCE_TYPE{i}"] = b.get("bucket_unit_type") or ""
        out[f"EL_BUCKET_CUR_BALANCE{i}"] = b.get("bucket_cur_balance") or ""
        out[f"EL_BUCKET_CHG_BALANCE{i}"] = b.get("bucket_chg_balance") or ""
        out[f"EL_BUCKET_RATE_ID{i}"] = b.get("bucket_rate_id") or ""

    # calling party and apn
    msisdn, imsi = extract_subscription_ids(generic)
    out["EL_CALLING_PARTY_NUMBER"] = msisdn or ""
    out["EL_APN"] = elems.get("accessPointName") or ""
    out["EL_URL"] = ""
    out["EL_CALLING_PARTY_IMSI"] = imsi or ""

    # flux and duration
    out["EL_TOTAL_FLUX"] = rate_usage
    out["EL_UP_FLUX"] = ""
    out["EL_DOWN_FLUX"] = ""
    out["EL_ELAPSE_DURATION"] = elems.get("duration") or ""

    # IMEI
    out["EL_IMEI"] = ""
    out["EL_BEARER_PROTOCOL_TYPE"] = ""

    # main offering id: join bundleName across group_subs
    bundles = []
    for s in group_subs:
        bn = safe_get(s, ["recordElements","bundleName"]) or ""
        if bn:
            bundles.append(bn)
    out["EL_MAIN_OFFERING_ID"] = ",".join(bundles) if bundles else ""

    out["EL_PAY_TYPE"] = cbl.get("EL_PRE_POST") if isinstance(cbl, dict) else ""

    # charging type: first chargingServiceInfo.chargingServiceType
    charging_type = ""
    for s in group_subs:
        for charge in (s.get("recordSubExtensions", []) or []):
            if charge.get("recordProperty") == "chargingServiceInfo":
                charging_type = safe_get(charge, ["recordElements","chargingServiceType"]) or ""
                if charging_type:
                    break
        if charging_type:
            break
    out["EL_CHARGING_TYPE"] = charging_type

    out["EL_ROAM_STATE"] = elems.get("roamingIndicator") or ""
    out["EL_CALLING_VPN_TOP_GROUP_NUMBER"] = ""
    out["EL_CALLING_VPN_GROUP_NUMBER"] = ""
    out["EL_START_TIME_OF_BILL_CYCLE"] = elems.get("recordOpeningTime") or ""
    out["EL_LAST_EFFECT_OFFERING"] = ""
    out["EL_RATING_GROUP"] = safe_get(list_of_mscc, ["recordSubExtensions",0,"recordElements","ratingGroup"]) or ""
    out["EL_USER_STATE"] = elems.get("deviceState") or ""
    out["EL_RAT_TYPE"] = elems.get("rATType") or ""
    out["EL_CHARGE_PARTY_INDICATOR"] = ""
    out["EL_COUNTRY_NAME"] = ""
    out["EL_PAY_DEFAULT_ACCT_ID"] = ""

    # taxes from first account and first bucket
    out["EL_TAX1"] = acct_slots[0].get("committedTaxAmount") if acct_slots else ""
    out["EL_TAX2"] = bucket_slots[0].get("committedTaxAmount") if bucket_slots else ""

    # location
    if str(elems.get("rATType", "")) == "6":
        out["EL_LOCATION"] = decode_location_hex_field(elems.get("userLocationInformation", ""))
    else:
        tail = last_n_chars(elems.get("userLocationInformation", ""), 14)
        if tail:
            out["EL_LOCATION"] = f"{tail[0:6]}-{tail[6:10]}-{tail[10:14]}"
        else:
            out["EL_LOCATION"] = ""

    # alternate ids from group_subs
    alt_ids = []
    for s in group_subs:
        a = safe_get(s, ["recordElements","alternateId"]) or ""
        if a:
            alt_ids.append(str(a))
    out["EL_ALTERNATE_ID"] = "~".join(alt_ids) if alt_ids else ""

    out["EL_BUSINESS_TYPE"] = ""
    out["EL_SUBSCRIBER_KEY"] = ""
    out["EL_ACCOUNT_KEY"] = ""

    # additionalBalanceInfo: aggregate similar to other mappers
    add_infos = []
    for s in group_subs:
        for charge in (s.get("recordSubExtensions", []) or []):
            if charge.get("recordProperty") != "chargingServiceInfo":
                continue
            for nested in (charge.get("recordSubExtensions", []) or []):
                if nested.get("recordProperty") == "additionalBalanceInfo":
                    abe = nested.get("recordElements", {}) or {}
                    bi = abe.get("bucketInfo", {}) or {}
                    add_infos.append({
                        "chargingServiceName": abe.get("chargingServiceName") or "",
                        "usageType": abe.get("usageType") or "",
                        "usedAs": abe.get("usedAs") or "",
                        "bucketName": bi.get("bucketName") or "",
                        "bucketUnitType": bi.get("bucketUnitType") or "",
                        "bucketBalanceBefore": bi.get("bucketBalanceBefore") or "",
                        "bucketBalanceAfter": bi.get("bucketBalanceAfter") or "",
                        "carryOverBucket": bi.get("carryOverBucket") or "",
                        "bucketCommitedUnits": bi.get("bucketCommitedUnits") or "",
                        "rateId": bi.get("rateId") or "",
                        "committedTaxAmount": bi.get("committedTaxAmount") or "",
                        "totalTaxAmount": bi.get("totalTaxAmount") or "",
                        "tariffID": bi.get("tariffID") or "",
                        "totalVolumeCharged": bi.get("totalVolumeCharged") or "",
                        "roundedVolumeCharged": bi.get("roundedVolumeCharged") or "",
                        "deltaVolume": bi.get("deltaVolume") or "",
                    })
    def j(k):
        return ",".join(a.get(k, "") for a in add_infos if a.get(k) not in (None, ""))

    out["EL_ADDITIONALBALANCEINFO_CHARGINGSERVICENAME"] = j("chargingServiceName")
    out["EL_ADDITIONALBALANCEINFO_USAGETYPE"] = j("usageType")
    out["EL_ADDITIONALBALANCEINFO_USEDAS"] = j("usedAs")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETNAME"] = j("bucketName")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETUNITTYPE"] = j("bucketUnitType")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETKINDOFUNIT"] = ""
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEBEFORE"] = j("bucketBalanceBefore")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETBALANCEAFTER"] = j("bucketBalanceAfter")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_CARRYOVERBUCKET"] = j("carryOverBucket")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETCOMMITEDUNITS"] = j("bucketCommitedUnits")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_BUCKETRESERVEDUNITS"] = ""
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_RATEID"] = j("rateId")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_PRIMARYCOSTCOMMITTED"] = ""
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_SECONDARYCOSTCOMMITTED"] = ""
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXATIONID"] = ""
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TAXRATEAPPLIED"] = ""
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_COMMITTEDTAXAMOUNT"] = j("committedTaxAmount")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALTAXAMOUNT"] = j("totalTaxAmount")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TARIFFID"] = j("tariffID")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_TOTALVOLUMECHARGED"] = j("totalVolumeCharged")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_ROUNDEDVOLUMECHARGED"] = j("roundedVolumeCharged")
    out["EL_ADDITIONALBALANCEINFO_BUCKETINFO_DELTAVOLUME"] = j("deltaVolume")

    # Unlimited bundle rules (112..114) - check group_subs
    out["EL_UNLTD_BUNDLE_NAME"] = ""
    out["EL_UNLTD_TOTAL_VOLUME_CHARGED"] = ""
    out["EL_UNLTD_BUNDLE_UNIT_TYPE"] = ""
    for s, acct in zip(group_subs, acct_slots):
        has_bucket = False
        for charge in (s.get("recordSubExtensions", []) or []):
            if charge.get("recordProperty") != "chargingServiceInfo":
                continue
            for csub in (charge.get("recordSubExtensions", []) or []):
                if csub.get("recordProperty") == "bucketInfo":
                    has_bucket = True
                    break
            if has_bucket:
                break
        acct_committed = to_decimal(acct.get("accountBalanceCommitted"))
        total_vol = acct.get("totalVolumeCharged") or ""
        if (not has_bucket) and (acct_committed is not None and acct_committed == 0) and (total_vol not in (None, "", "0")):
            out["EL_UNLTD_BUNDLE_NAME"] = safe_get(s, ["recordElements","bundleName"]) or ""
            out["EL_UNLTD_TOTAL_VOLUME_CHARGED"] = total_vol
            out["EL_UNLTD_BUNDLE_UNIT_TYPE"] = "VOLUME"
            break

    # ORIG LOCATION
    if str(elems.get("rATType", "")) == "6":
        out["EL_ORIG_LOCATION"] = decode_location_hex_field(elems.get("origUserLocationInfo", ""))
    else:
        tail = last_n_chars(elems.get("origUserLocationInfo", ""), 14)
        if tail:
            out["EL_ORIG_LOCATION"] = f"{tail[0:6]}-{tail[6:10]}-{tail[10:14]}"
        else:
            out["EL_ORIG_LOCATION"] = ""

    return out

# File processing

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
            m = map_group_data(rec)
            mapped.append(m)
        except Exception as e:
            logger.exception("Mapping failed")
            rejects.append({"reason": str(e), "record": rec})

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{path.stem}_group_phase1.json"
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

# CLI
if __name__ == '__main__':
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
