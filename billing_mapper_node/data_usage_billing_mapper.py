import json
import time
import shutil
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from decimal import Decimal, InvalidOperation
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("billing_data_cdr")

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


def fmt_decimal(d: Optional[Decimal]) -> str:
    if d is None:
        return ""
    try:
        # keep 5 decimal places if present
        return format(d.quantize(Decimal("0.00000")), 'f')
    except Exception:
        return str(d)


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


def parse_generation_ts(s: str) -> str:
    if not s:
        return ""
    # input examples: '15/08/2025 15:26:53+03:00' or '15/08/2025 15:26:53'
    try:
        # normalize timezone format +03:00 -> +0300 for %z
        ss = s.strip()
        if ss.endswith('Z'):
            ss = ss.replace('Z', '+0000')
        if '+' in ss[-6:] or '-' in ss[-6:]:
            # replace the last colon in timezone if present
            if ss[-3] == ':' and (ss[-6] == '+' or ss[-6] == '-'):
                ss = ss[:-3] + ss[-2:]
        # try parse with timezone
        try:
            dt = datetime.strptime(ss, '%d/%m/%Y %H:%M:%S%z')
        except Exception:
            dt = datetime.strptime(ss.split('+')[0].strip(), '%d/%m/%Y %H:%M:%S')
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return ""

# record traversal helpers

def find_list_of_mscc(rec: dict) -> Optional[dict]:
    for ext in rec.get('recordExtensions', []) or []:
        if ext.get('recordProperty') == 'listOfMscc':
            return ext
    return None


def collect_subscription_blocks(mscc_block: Optional[dict]) -> List[dict]:
    subs = []
    if not mscc_block:
        return subs
    for sub in mscc_block.get('recordSubExtensions', []) or []:
        if sub.get('recordProperty') == 'mscc':
            for dev in sub.get('recordSubExtensions', []) or []:
                if dev.get('recordProperty') == 'deviceInfo':
                    for sinfo in dev.get('recordSubExtensions', []) or []:
                        if sinfo.get('recordProperty') == 'subscriptionInfo':
                            subs.append(sinfo)
    return subs

# billing mapper

def map_billing(cdr_json: Dict[str, Any]) -> Dict[str, Any]:
    generic = safe_get(cdr_json, ['original','payload','genericRecord']) or cdr_json
    elems = safe_get(generic, ['recordElements']) or {}
    cbl = cdr_json.get('CBL_TAG') or {}

    out: Dict[str, Any] = {}

    # find listOfMscc and subs
    list_of_mscc = find_list_of_mscc(generic)
    mscc_block = None
    if list_of_mscc:
        for sub in (list_of_mscc.get('recordSubExtensions', []) or []):
            if sub.get('recordProperty') == 'mscc':
                mscc_block = sub
                break
    subs = collect_subscription_blocks(list_of_mscc)

    # 1 EL_ACCOUNT_ID: subscriptionId type=0
    acc_id = ""
    for ext in generic.get('recordExtensions', []) or []:
        if ext.get('recordProperty') == 'listOfSubscriptionID':
            for s in ext.get('recordSubExtensions', []) or []:
                if s.get('recordProperty') != 'subscriptionId':
                    continue
                re = s.get('recordElements', {}) or {}
                dtype = str(re.get('subscriptionIdType') or re.get('subscriptionIDType') or '')
                data = re.get('subscriptionIdData') or re.get('subscriptionIDData') or ''
                if dtype == '0':
                    acc_id = data
                    break
        if acc_id:
            break
    out['EL_ACCOUNT_ID'] = acc_id

    # 2 EL_DIALLED_DIGITS null
    out['EL_DIALLED_DIGITS'] = ""

    # 3 EL_EVENT_LABEL from CBL or recordElements
    out['EL_EVENT_LABEL'] = cbl.get('EL_EVENT_LABEL_VAL') if isinstance(cbl, dict) else elems.get('EL_EVENT_LABEL_VAL', '')

    # 4 duration
    out['EL_CALL_DURATION'] = elems.get('duration') or ''

    # 27 EL_TAX_AMOUNT from committedTaxAmount in first accountInfo
    tax_amount = None
    gross_amount = None
    # find first accountInfo in subs
    for s in subs:
        for charge in s.get('recordSubExtensions', []) or []:
            if charge.get('recordProperty') != 'chargingServiceInfo':
                continue
            for csub in charge.get('recordSubExtensions', []) or []:
                if csub.get('recordProperty') == 'accountInfo':
                    acc = csub.get('recordElements', {}) or {}
                    tax_amount = to_decimal(acc.get('committedTaxAmount'))
                    # EL_GROSS_CALL_COST per rule (26)
                    gross_amount = to_decimal(acc.get('accountBalanceCommittedBR') or acc.get('accountBalanceCommitted'))
                    if gross_amount is None:
                        before = to_decimal(acc.get('accountBalanceBefore'))
                        after = to_decimal(acc.get('accountBalanceAfter'))
                        if before is not None and after is not None:
                            gross_amount = before - after
                    break
            if gross_amount is not None or tax_amount is not None:
                break
        if gross_amount is not None or tax_amount is not None:
            break

    out['EL_TAX_AMOUNT'] = fmt_decimal(tax_amount)
    out['EL_GROSS_CALL_COST'] = fmt_decimal(gross_amount)

    # 5 EL_CALL_COST = EL_GROSS_CALL_COST - EL_TAX_AMOUNT
    call_cost = None
    if gross_amount is not None:
        call_cost = gross_amount - (tax_amount or Decimal(0))
    out['EL_CALL_COST'] = fmt_decimal(call_cost)

    # 6 EL_ROAMING_INDICATOR: RoamingStatus HOME->0 else 1
    roaming = (elems.get('RoamingStatus') or elems.get('roamingIndicator') or '')
    if isinstance(roaming, str) and roaming.upper() == 'HOME':
        out['EL_ROAMING_INDICATOR'] = 0
    elif roaming == '':
        out['EL_ROAMING_INDICATOR'] = ""
    else:
        out['EL_ROAMING_INDICATOR'] = 1

    # 7 EL_CALL_VOLUME listOfMscc.mscc.totalVolumeConsumed
    call_vol = ''
    if mscc_block:
        call_vol = safe_get(mscc_block, ['recordElements','totalVolumeConsumed']) or safe_get(mscc_block, ['recordElements','timeUsage']) or ''
    out['EL_CALL_VOLUME'] = call_vol

    # 8 EL_BAND_LABEL_AMA_CODE fixed "onnet"
    out['EL_BAND_LABEL_AMA_CODE'] = 'onnet'

    # 9 EL_APPLIED_DISCOUNT_ID: iterate bucketInfo.bucketName up to 5 separated by |
    applied = []
    for s in subs:
        for charge in s.get('recordSubExtensions', []) or []:
            if charge.get('recordProperty') != 'chargingServiceInfo':
                continue
            for csub in charge.get('recordSubExtensions', []) or []:
                if csub.get('recordProperty') == 'bucketInfo':
                    b = csub.get('recordElements', {}) or {}
                    bn = b.get('bucketName')
                    if bn:
                        applied.append(str(bn))
                    if len(applied) >= 5:
                        break
            if len(applied) >= 5:
                break
        if len(applied) >= 5:
            break
    out['EL_APPLIED_DISCOUNT_ID'] = '|'.join(applied)

    # 10 EL_EVENT_LABEL duplicate
    out['EL_EVENT_LABEL_2'] = out['EL_EVENT_LABEL']

    # 11 EL_ORIGINATING_ZONE_CODE null
    out['EL_ORIGINATING_ZONE_CODE'] = ""

    # 12 EL_PROCESSED_TIMESTAMP - formatted generationTimestamp
    out['EL_PROCESSED_TIMESTAMP'] = parse_generation_ts(elems.get('generationTimestamp') or '')

    # 13 EL_PLAN_ID bundleName from subs joined (first matching mscc.deviceInfo.subscriptionInfo.bundleName)
    plans = []
    for s in subs:
        bn = safe_get(s, ['recordElements','bundleName']) or ''
        if bn:
            plans.append(bn)
    out['EL_PLAN_ID'] = ','.join(plans)

    # 14..15 PEAK/OFF_PEAK null
    out['EL_PEAK'] = ""
    out['EL_OFF_PEAK'] = ""

    # 16 EL_EVENT_RESULT: resultCode mapping
    res_code = elems.get('resultCode') or safe_get(mscc_block, ['recordElements','resultCode']) or ''
    try:
        rc_int = int(str(res_code))
    except Exception:
        rc_int = None
    success_codes = {2001, 4012}
    if rc_int in success_codes:
        out['EL_EVENT_RESULT'] = 1
    else:
        out['EL_EVENT_RESULT'] = res_code

    # 17 EL_GENERATION_TIMESTAMP same as processed timestamp
    out['EL_GENERATION_TIMESTAMP'] = out['EL_PROCESSED_TIMESTAMP']

    # 18 EL_PROCESS_FILENAME metadata - try common locations
    proc_fn = ''
    proc_fn = safe_get(cdr_json, ['metadata','filename']) or safe_get(cdr_json, ['_metadata','filename']) or ''
    out['EL_PROCESS_FILENAME'] = proc_fn

    # 19 EL_CUG_ENABLED fixed "false"
    out['EL_CUG_ENABLED'] = 'false'

    # 20 EL_APPLIED_FAMILY_GROUP_DISCOUNT_IDS null
    out['EL_APPLIED_FAMILY_GROUP_DISCOUNT_IDS'] = ""

    # 21 EL_POSTPAIDBUCKETID: bundleName list from subs
    out['EL_POSTPAIDBUCKETID'] = ','.join(plans)

    # 22 EL_POSTPAIDBUCKETUSAGES: bucketBalanceBefore - bucketBalanceAfter for bucketInfo entries, join by comma
    usages = []
    for s in subs:
        for charge in s.get('recordSubExtensions', []) or []:
            if charge.get('recordProperty') != 'chargingServiceInfo':
                continue
            for csub in charge.get('recordSubExtensions', []) or []:
                if csub.get('recordProperty') == 'bucketInfo':
                    b = csub.get('recordElements', {}) or {}
                    before = to_decimal(b.get('bucketBalanceBefore'))
                    after = to_decimal(b.get('bucketBalanceAfter'))
                    if before is not None and after is not None:
                        diff = before - after
                        usages.append(fmt_decimal(diff))
    out['EL_POSTPAIDBUCKETUSAGES'] = ','.join(usages)

    # 23 EL_CDR_REFERENCE_NUMBER sessionId
    out['EL_CDR_REFERENCE_NUMBER'] = elems.get('sessionId') or ''

    # 24 EL_ROUNDED_CALL_DURATION duration
    out['EL_ROUNDED_CALL_DURATION'] = elems.get('duration') or ''

    # 25 EL_ROUNDED_CALL_VOLUME: prefer bucket roundedVolumeCharged else account roundedVolumeCharged
    rounded_vol = ''
    found = False
    for s in subs:
        for charge in s.get('recordSubExtensions', []) or []:
            if charge.get('recordProperty') != 'chargingServiceInfo':
                continue
            for csub in charge.get('recordSubExtensions', []) or []:
                if csub.get('recordProperty') == 'bucketInfo':
                    b = csub.get('recordElements', {}) or {}
                    rv = b.get('roundedVolumeCharged')
                    if rv:
                        rounded_vol = str(rv)
                        found = True
                        break
            if found:
                break
        if found:
            break
    if not found:
        for s in subs:
            for charge in s.get('recordSubExtensions', []) or []:
                if charge.get('recordProperty') != 'chargingServiceInfo':
                    continue
                for csub in charge.get('recordSubExtensions', []) or []:
                    if csub.get('recordProperty') == 'accountInfo':
                        a = csub.get('recordElements', {}) or {}
                        rv = a.get('roundedVolumeCharged')
                        if rv:
                            rounded_vol = str(rv)
                            found = True
                            break
                if found:
                    break
            if found:
                break
    out['EL_ROUNDED_CALL_VOLUME'] = rounded_vol

    # 26 EL_GROSS_CALL_COST already set

    # 28 & 29 null
    out['EL_CHARGE_CODE'] = ""
    out['EL_PLAN_NAME'] = ""

    return out

# File processing CLI

def read_json_stable(path: Path, retries: int = 5, delay: float = 0.2) -> dict:
    for i in range(retries):
        try:
            with path.open('r', encoding='utf-8') as f:
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
    if isinstance(data, dict) and 'records' in data and isinstance(data['records'], dict):
        records = list(data['records'].values())
    elif isinstance(data, list):
        records = data
    else:
        records = [data]

    mapped = []
    rejects = []
    for rec in records:
        try:
            m = map_billing(rec)
            mapped.append(m)
        except Exception as e:
            logger.exception('Mapping failed')
            rejects.append({'reason': str(e), 'record': rec})

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{path.stem}_billing_phase1.json"
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
