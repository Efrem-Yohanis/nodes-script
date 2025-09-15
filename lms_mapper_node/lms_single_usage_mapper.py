import json
import time
import shutil
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from decimal import Decimal, InvalidOperation
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("lms_individual_billing_cdr")

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

# traversal helpers

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


def normalize_msisdn_for_roaming(original: str) -> str:
    if not original:
        return ''
    s = str(original)
    if s.startswith('0'):
        s = s[1:]
    if not s.startswith('251'):
        s = '251' + s
    return s

# find first subscriptionInfo where accountInfo exists and bucketInfo does not

def find_first_account_only(subs: List[dict]) -> Optional[dict]:
    for s in subs:
        has_account = False
        has_bucket = False
        acct_elems = None
        for charge in (s.get('recordSubExtensions', []) or []):
            if charge.get('recordProperty') != 'chargingServiceInfo':
                continue
            for csub in (charge.get('recordSubExtensions', []) or []):
                if csub.get('recordProperty') == 'accountInfo':
                    has_account = True
                    acct_elems = csub.get('recordElements', {}) or {}
                if csub.get('recordProperty') == 'bucketInfo':
                    has_bucket = True
        if has_account and not has_bucket:
            return acct_elems
    return None

# find additionalBalanceInfo bucket committed units

def find_additional_bucket_committed_units(subs: List[dict]) -> Optional[Decimal]:
    for s in subs:
        for charge in (s.get('recordSubExtensions', []) or []):
            if charge.get('recordProperty') != 'chargingServiceInfo':
                continue
            for csub in (charge.get('recordSubExtensions', []) or []):
                if csub.get('recordProperty') == 'additionalBalanceInfo':
                    abe = csub.get('recordElements', {}) or {}
                    bi = abe.get('bucketInfo') or {}
                    val = bi.get('bucketCommitedUnits') or bi.get('bucketCommitedUnits')
                    if val not in (None, ''):
                        return to_decimal(val)
    return None

# usage volume selectors

def find_additional_usage_secondary(subs: List[dict]) -> Optional[str]:
    # priority: totalVolumeCharged, totalTimeCharged, totalUnitsCharged
    for s in subs:
        for charge in (s.get('recordSubExtensions', []) or []):
            if charge.get('recordProperty') != 'chargingServiceInfo':
                continue
            for csub in (charge.get('recordSubExtensions', []) or []):
                if csub.get('recordProperty') == 'additionalBalanceInfo':
                    abe = csub.get('recordElements', {}) or {}
                    bi = abe.get('bucketInfo') or {}
                    if bi.get('totalVolumeCharged') not in (None, ''):
                        return str(bi.get('totalVolumeCharged'))
                    if bi.get('totalTimeCharged') not in (None, ''):
                        return str(bi.get('totalTimeCharged'))
                    if bi.get('totalUnitsCharged') not in (None, ''):
                        return str(bi.get('totalUnitsCharged'))
    return None


def find_normal_usage(subs: List[dict], record_type: str) -> Optional[str]:
    # find first subscriptionInfo where only accountInfo exists
    acct = find_first_account_only(subs)
    if not acct:
        return None
    if record_type == 'DATA':
        v = acct.get('roundedVolumeCharged') or acct.get('roundedVolumeCharged')
        return str(v) if v not in (None, '') else None
    if record_type in ('VOICE', 'VOICE_CALL', 'PS'):
        v = acct.get('TotalTimeCharged') or acct.get('totalTimeCharged') or acct.get('TotalTimeCharged')
        return str(v) if v not in (None, '') else None
    if record_type in ('SMS', 'USSD', 'ECOMMERCE'):
        v = acct.get('totalUnitsCharged') or acct.get('TotalUnitsCharged')
        return str(v) if v not in (None, '') else None
    return None

# mapper

def map_lms_individual(cdr_json: Dict[str, Any]) -> Dict[str, Any]:
    generic = safe_get(cdr_json, ['original','payload','genericRecord']) or cdr_json
    elems = safe_get(generic, ['recordElements']) or {}
    cbl = cdr_json.get('CBL_TAG') or {}

    out: Dict[str, Any] = {}

    # EL_LMS from EL_SUCCESS
    el_success = cbl.get('EL_SUCCESS') if isinstance(cbl, dict) else None
    out['EL_LMS'] = 1 if el_success == 1 else 0

    # collect mscc and subscription blocks
    list_of_mscc = find_list_of_mscc(generic)
    subs = collect_subscription_blocks(list_of_mscc)

    # record event/subtype
    mscc_block = None
    if list_of_mscc:
        for s in (list_of_mscc.get('recordSubExtensions', []) or []):
            if s.get('recordProperty') == 'mscc':
                mscc_block = s
                break
    record_event_type = safe_get(mscc_block, ['recordElements','recordEventType']) or safe_get(mscc_block, ['recordElements','subRecordEventType']) or ''

    # 2 EL_MSISDN
    msisdn = ''
    for ext in (generic.get('recordExtensions', []) or []):
        if ext.get('recordProperty') == 'listOfSubscriptionID':
            for s in (ext.get('recordSubExtensions', []) or []):
                if s.get('recordProperty') != 'subscriptionId':
                    continue
                re = s.get('recordElements', {}) or {}
                stype = str(re.get('subscriptionIdType') or re.get('subscriptionIDType') or '')
                data = re.get('subscriptionIdData') or re.get('subscriptionIDData') or ''
                if stype == '0' and not msisdn:
                    msisdn = data
    # Determine which service category to use originatorAddress logic
    rec_type_upper = (elems.get('CDRTagCategory') or elems.get('CDRTag') or '').upper()
    # For Voice use roaming and subrecord event conditions
    if rec_type_upper.startswith('VOICE') or record_event_type in ('MTC','FWD','MT') or elems.get('recordType','').upper().find('VOICE')>=0:
        roaming_indicator = elems.get('roamingIndicator') or elems.get('RoamingStatus') or ''
        if ((str(roaming_indicator).upper() == 'ROAMING' and str(record_event_type).upper() == 'MTC') or (str(record_event_type).upper() == 'FWD')):
            normalized = msisdn
            if normalized and not normalized.startswith('251') and len(normalized) < 10:
                normalized = normalize_msisdn_for_roaming(normalized)
            out['EL_MSISDN'] = normalized or msisdn or ''
        else:
            out['EL_MSISDN'] = msisdn or ''
    else:
        # SMS, USSD, MMS use originatorAddress
        if rec_type_upper.startswith('SMS') or rec_type_upper.startswith('USSD') or rec_type_upper.startswith('MMS'):
            out['EL_MSISDN'] = elems.get('originatorAddress') or elems.get('callingPartyAddress') or ''
        elif rec_type_upper.startswith('ECOM') or rec_type_upper.startswith('ECOMMERCE'):
            out['EL_MSISDN'] = msisdn or ''
        else:
            out['EL_MSISDN'] = msisdn or ''

    # 3 EL_DIALLED_DIGITS
    if rec_type_upper.startswith('DATA') or rec_type_upper.startswith('ECOMMERCE'):
        out['EL_DIALLED_DIGITS'] = ''
    elif rec_type_upper.startswith('VOICE'):
        roaming_indicator = elems.get('roamingIndicator') or elems.get('RoamingStatus') or ''
        calling = elems.get('callingPartyAddress') or elems.get('originatorAddress') or elems.get('Aparty') or ''
        called = elems.get('calledPartyAddress') or elems.get('recipientAddress') or elems.get('Bparty') or ''
        if ((str(roaming_indicator).upper() == 'ROAMING' and str(record_event_type).upper() == 'MTC') or (str(record_event_type).upper() == 'FWD')):
            if calling and not str(calling).startswith('251') and len(str(calling)) < 10:
                out['EL_DIALLED_DIGITS'] = normalize_msisdn_for_roaming(calling)
            else:
                out['EL_DIALLED_DIGITS'] = calling
        else:
            out['EL_DIALLED_DIGITS'] = called
    else:
        # SMS/MMS/USSD
        out['EL_DIALLED_DIGITS'] = elems.get('recipientAddress') or elems.get('calledPartyAddress') or ''

    # 4 EVENT_LABEL
    out['EVENT_LABEL'] = cbl.get('EL_EVENT_LABEL_VAL') if isinstance(cbl, dict) else elems.get('EL_EVENT_LABEL_VAL','')

    # 5 EL_CALL_COST: priority additionalBalanceInfo.bucketInfo.bucketCommitedUnits -> accountBalanceCommittedBR from first accountOnly -> before-after
    call_cost_val = None
    add_committed = find_additional_bucket_committed_units(subs)
    if add_committed is not None:
        call_cost_val = add_committed
    else:
        acct_only = find_first_account_only(subs)
        if acct_only:
            g = to_decimal(acct_only.get('accountBalanceCommitted') or acct_only.get('accountBalanceCommittedBR'))
            if g is None:
                bef = to_decimal(acct_only.get('accountBalanceBefore'))
                aft = to_decimal(acct_only.get('accountBalanceAfter'))
                if bef is not None and aft is not None:
                    g = bef - aft
            call_cost_val = g
        else:
            # fallback: search any accountInfo
            for s in subs:
                for charge in (s.get('recordSubExtensions', []) or []):
                    if charge.get('recordProperty') != 'chargingServiceInfo':
                        continue
                    for csub in (charge.get('recordSubExtensions', []) or []):
                        if csub.get('recordProperty') == 'accountInfo':
                            acc = csub.get('recordElements', {}) or {}
                            g = to_decimal(acc.get('accountBalanceCommitted') or acc.get('accountBalanceCommittedBR'))
                            if g is None:
                                bef = to_decimal(acc.get('accountBalanceBefore'))
                                aft = to_decimal(acc.get('accountBalanceAfter'))
                                if bef is not None and aft is not None:
                                    g = bef - aft
                            if g is not None:
                                call_cost_val = g
                                break
                    if call_cost_val is not None:
                        break
                if call_cost_val is not None:
                    break
    out['EL_CALL_COST'] = fmt_decimal(call_cost_val)

    # 6 EL_ROAMING_INDICATOR direct mapping
    ri = elems.get('roamingIndicator') or elems.get('RoamingStatus') or ''
    out['EL_ROAMING_INDICATOR'] = 0 if isinstance(ri, str) and ri.upper() == 'HOME' else ('' if ri == '' else 1)

    # 7 EL_USAGE_VOLUME
    usage = None
    # additionalBalanceInfo primary
    add_primary = find_additional_bucket_committed_units(subs)
    if add_primary is not None:
        usage = fmt_decimal(add_primary)
    else:
        # secondary balances
        sec = find_additional_usage_secondary(subs)
        if sec is not None:
            usage = sec
        else:
            # normal case
            # determine recordSubType/recordType: try mscc.recordElements.recordEventType or generic.recordElements.DestinationType
            record_subtype = (safe_get(mscc_block, ['recordElements','recordEventType']) or safe_get(mscc_block, ['recordElements','recordEventType']) or elems.get('CDRTagCategory') or '').upper()
            norm = find_normal_usage(subs, record_subtype)
            if norm is not None:
                usage = norm
    out['EL_USAGE_VOLUME'] = usage or ''

    # 8 process filename
    out['EL_PROCESS_FILENAME'] = safe_get(cdr_json, ['metadata','filename']) or ''

    # 9 usage type
    usage_type_val = ''
    if record_event_type:
        if record_event_type.upper() in ('VOICE','MTC','FWD') or elems.get('recordType','').upper().find('VOICE')>=0:
            usage_type_val = record_event_type
        elif elems.get('recordType','').upper().find('MMS')>=0:
            usage_type_val = safe_get(mscc_block, ['recordElements','totalUnitsConsumed']) or ''
        else:
            usage_type_val = record_event_type
    out['EL_USAGE_TYPE'] = usage_type_val

    # 10 EL_EVENT_DATE callAnswerTime -> recordOpeningTime -> generationTimestamp
    event_date = elems.get('callAnswerTime') or elems.get('recordOpeningTime') or elems.get('generationTimestamp') or ''
    out['EL_EVENT_DATE'] = parse_ts(event_date)

    # 11 EL_PLAN_ID: bundleName from first accountOnly else any bundleName
    plan_id = ''
    acct_only = find_first_account_only(subs)
    if acct_only and acct_only.get('bundleName'):
        plan_id = acct_only.get('bundleName')
    else:
        for s in subs:
            bn = safe_get(s, ['recordElements','bundleName']) or ''
            if bn:
                plan_id = bn
                break
    out['EL_PLAN_ID'] = plan_id

    out['EL_PLAN_NAME'] = ''
    out['EL_PLAN_TYPE'] = ''

    # 14 CDR reference number from SessionSequenceNumber
    out['EL_CDR_REFERENCE_NUMBER'] = elems.get('sessionSequenceNumber') or ''

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
            m = map_lms_individual(rec)
            mapped.append(m)
        except Exception as e:
            logger.exception('Mapping failed')
            rejects.append({'reason': str(e), 'record': rec})

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{path.stem}_lms_individual_phase1.json"
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
