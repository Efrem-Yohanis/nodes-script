import json
import time
import shutil
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from decimal import Decimal, InvalidOperation
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ussd_billing_cdr")

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
        return format(d.quantize(Decimal("0.00000")), 'f')
    except Exception:
        return str(d)


def parse_ts(s: str) -> str:
    if not s:
        return ""
    try:
        ss = s.strip()
        if ss.endswith('Z'):
            ss = ss.replace('Z', '+0000')
        if '+' in ss[-6:] or '-' in ss[-6:]:
            if ss[-3] == ':' and (ss[-6] == '+' or ss[-6] == '-'):
                ss = ss[:-3] + ss[-2:]
        try:
            dt = datetime.strptime(ss, '%d/%m/%Y %H:%M:%S%z')
        except Exception:
            dt = datetime.strptime(ss.split('+')[0].strip(), '%d/%m/%Y %H:%M:%S')
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return ""

# traversal

def find_list_of_mscc(rec: dict) -> Optional[dict]:
    for ext in rec.get('recordExtensions', []) or []:
        if ext.get('recordProperty') == 'listOfMscc':
            return ext
    return None


def collect_subscription_blocks(mscc_ext: Optional[dict]) -> List[dict]:
    out = []
    if not mscc_ext:
        return out
    for s in (mscc_ext.get('recordSubExtensions', []) or []):
        if s.get('recordProperty') == 'mscc':
            for dev in (s.get('recordSubExtensions', []) or []):
                if dev.get('recordProperty') == 'deviceInfo':
                    for sub in (dev.get('recordSubExtensions', []) or []):
                        if sub.get('recordProperty') == 'subscriptionInfo':
                            out.append(sub)
    return out

# band label helper

def band_label(a: str, b: str) -> str:
    if not a or not b:
        return 'onnet'
    a_s = str(a)
    b_s = str(b)
    if a_s.startswith('2517') and b_s.startswith('2517') and len(a_s) > 10 and len(b_s) > 10:
        return 'onnet'
    if a_s.startswith('2517') and b_s.startswith('251') and not b_s.startswith('2517') and len(a_s) > 10 and len(b_s) > 10:
        return 'offnet'
    if a_s.startswith('2517') and not b_s.startswith('251') and len(a_s) > 10 and len(b_s) > 10:
        return 'International'
    return 'onnet'

# aggregations

def collect_bucket_names(subs: List[dict], limit: int = 5) -> List[str]:
    out = []
    for s in subs:
        for charge in (s.get('recordSubExtensions', []) or []):
            if charge.get('recordProperty') != 'chargingServiceInfo':
                continue
            for csub in (charge.get('recordSubExtensions', []) or []):
                if csub.get('recordProperty') == 'bucketInfo':
                    b = csub.get('recordElements', {}) or {}
                    bn = b.get('bucketName')
                    if bn:
                        out.append(str(bn))
                        if len(out) >= limit:
                            return out
    return out


def collect_postpaid_usages(subs: List[dict]) -> List[str]:
    outs = []
    for s in subs:
        for charge in (s.get('recordSubExtensions', []) or []):
            if charge.get('recordProperty') != 'chargingServiceInfo':
                continue
            for csub in (charge.get('recordSubExtensions', []) or []):
                if csub.get('recordProperty') == 'bucketInfo':
                    b = csub.get('recordElements', {}) or {}
                    bef = to_decimal(b.get('bucketBalanceBefore'))
                    aft = to_decimal(b.get('bucketBalanceAfter'))
                    if bef is not None and aft is not None:
                        outs.append(fmt_decimal(bef - aft))
    return outs

# gross/tax finder

def find_first_account_for_gross(subs: List[dict]) -> Optional[dict]:
    for s in subs:
        for charge in (s.get('recordSubExtensions', []) or []):
            if charge.get('recordProperty') != 'chargingServiceInfo':
                continue
            for csub in (charge.get('recordSubExtensions', []) or []):
                if csub.get('recordProperty') == 'accountInfo':
                    return csub.get('recordElements', {}) or {}
    return None

# mapper

def map_ussd_billing(cdr_json: Dict[str, Any]) -> Dict[str, Any]:
    generic = safe_get(cdr_json, ['original','payload','genericRecord']) or cdr_json
    elems = safe_get(generic, ['recordElements']) or {}
    cbl = cdr_json.get('CBL_TAG') or {}

    out: Dict[str, Any] = {}

    list_of_mscc = find_list_of_mscc(generic)
    subs = collect_subscription_blocks(list_of_mscc)

    # 1 account id
    acc = ''
    for ext in (generic.get('recordExtensions', []) or []):
        if ext.get('recordProperty') == 'listOfSubscriptionID':
            for s in (ext.get('recordSubExtensions', []) or []):
                if s.get('recordProperty') != 'subscriptionId':
                    continue
                re = s.get('recordElements', {}) or {}
                stype = str(re.get('subscriptionIdType') or re.get('subscriptionIDType') or '')
                data = re.get('subscriptionIdData') or re.get('subscriptionIDData') or ''
                if stype == '0' and not acc:
                    acc = data
    out['EL_ACCOUNT_ID'] = acc or ''

    # 2 dialled digits - recipientAddress
    out['EL_DIALLED_DIGITS'] = elems.get('recipientAddress') or elems.get('calledPartyAddress') or ''

    # 3 event label
    out['EL_EVENT_LABEL'] = cbl.get('EL_EVENT_LABEL_VAL') if isinstance(cbl, dict) else elems.get('EL_EVENT_LABEL_VAL','')

    # 4 duration
    out['EL_CALL_DURATION'] = elems.get('duration') or ''

    # gross/tax
    acct = find_first_account_for_gross(subs)
    tax = None
    gross = None
    if acct:
        tax = to_decimal(acct.get('committedTaxAmount'))
        gross = to_decimal(acct.get('accountBalanceCommitted') or acct.get('accountBalanceCommittedBR'))
        if gross is None:
            bef = to_decimal(acct.get('accountBalanceBefore'))
            aft = to_decimal(acct.get('accountBalanceAfter'))
            if bef is not None and aft is not None:
                gross = bef - aft
    out['EL_TAX_AMOUNT'] = fmt_decimal(tax)
    out['EL_GROSS_CALL_COST'] = fmt_decimal(gross)
    call_cost = None
    if gross is not None:
        call_cost = gross - (tax or Decimal(0))
    out['EL_CALL_COST'] = fmt_decimal(call_cost)

    # 6 roaming indicator
    roaming = elems.get('RoamingStatus') or elems.get('roamingIndicator') or ''
    if isinstance(roaming, str) and roaming.upper() == 'HOME':
        out['EL_ROAMING_INDICATOR'] = 0
    elif roaming == '':
        out['EL_ROAMING_INDICATOR'] = ''
    else:
        out['EL_ROAMING_INDICATOR'] = 1

    # 7 call volume null
    out['EL_CALL_VOLUME'] = ""

    # 8 band label
    a = elems.get('originatorAddress') or elems.get('callingPartyAddress') or elems.get('Aparty') or ''
    b = elems.get('recipientAddress') or elems.get('calledPartyAddress') or elems.get('Bparty') or ''
    out['EL_BAND_LABEL_AMA_CODE'] = band_label(a, b)

    # 9 applied discounts
    applied = collect_bucket_names(subs, limit=5)
    out['EL_APPLIED_DISCOUNT_ID'] = '|'.join(applied)

    out['EL_EVENT_LABEL_2'] = out['EL_EVENT_LABEL']
    out['EL_ORIGINATING_ZONE_CODE'] = ''

    out['EL_PROCESSED_TIMESTAMP'] = parse_ts(elems.get('generationTimestamp') or '')

    plans = []
    for s in subs:
        bn = safe_get(s, ['recordElements','bundleName']) or ''
        if bn:
            plans.append(bn)
    out['EL_PLAN_ID'] = ','.join(plans)

    out['EL_PEAK'] = ''
    out['EL_OFF_PEAK'] = ''

    res_code = elems.get('resultCode') or safe_get(list_of_mscc, ['recordSubExtensions',0,'recordElements','resultCode']) or ''
    try:
        rc_int = int(str(res_code))
    except Exception:
        rc_int = None
    success_codes = {2001, 4012}
    out['EL_EVENT_RESULT'] = 1 if rc_int in success_codes else res_code

    out['EL_GENERATION_TIMESTAMP'] = out['EL_PROCESSED_TIMESTAMP']
    out['EL_PROCESS_FILENAME'] = safe_get(cdr_json, ['metadata','filename']) or ''
    out['EL_CUG_ENABLED'] = 'false'
    out['EL_APPLIED_FAMILY_GROUP_DISCOUNT_IDS'] = ''

    out['EL_POSTPAIDBUCKETID'] = ','.join(plans)
    out['EL_POSTPAIDBUCKETUSAGES'] = '|'.join(collect_postpaid_usages(subs))

    out['EL_CDR_REFERENCE_NUMBER'] = elems.get('sessionId') or ''
    out['EL_ROUNDED_CALL_DURATION'] = elems.get('duration') or ''
    out['EL_ROUNDED_CALL_VOLUME'] = ''
    out['EL_CHARGE_CODE'] = ''
    out['EL_PLAN_NAME'] = ''

    return out

# File processing

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
            m = map_ussd_billing(rec)
            mapped.append(m)
        except Exception as e:
            logger.exception('Mapping failed')
            rejects.append({'reason': str(e), 'record': rec})

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{path.stem}_ussd_billing_phase1.json"
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
