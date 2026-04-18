/**
 * Asymmetric setup flag calculator — mirrors the criteria originally in engine.py.
 * All inputs are already present in scan result rows returned by the API.
 * Returns an object with 5 fields that can be spread onto a row.
 */
export function calcAsymmetricFlags(row) {
  const price  = row.price;
  const ivRank = row.iv_rank;
  const spread = row.bid_ask_spread_pct;
  const oi     = row.open_interest;

  const s1Dist = (row.support_1 != null && price && price > 0)
    ? (price - row.support_1) / price * 100
    : null;

  const r1Dist = (row.resistance_1 != null && price && price > 0)
    ? (row.resistance_1 - price) / price * 100
    : null;

  // ASYMMETRIC_CC_FLAG — golden cross + uptrend/PD + near R1 + IV 40-80 + tight market + $2+ call + S1 floor
  const asymmetric_cc_flag = Boolean(
    row.sma_golden_cross === true
    && (row.sma_regime === "UPTREND" || row.resistance_1 == null)
    && (row.resistance_1 == null || (r1Dist != null && r1Dist <= 10))
    && ivRank != null && ivRank >= 40 && ivRank <= 80
    && spread != null && spread <= 0.10
    && oi != null && oi >= 200
    && row.atm_call_premium != null && row.atm_call_premium >= 2.00
    && s1Dist != null && s1Dist <= 12
  );

  // ASYMMETRIC_CSP_FLAG — golden cross + price near strong support + IV 45+ + $2+ put
  const asymmetric_csp_flag = Boolean(
    row.sma_golden_cross === true
    && s1Dist != null && s1Dist <= 8
    && row.support_1_strength != null && row.support_1_strength >= 8
    && ivRank != null && ivRank >= 45
    && spread != null && spread <= 0.10
    && oi != null && oi >= 200
    && row.atm_put_premium != null && row.atm_put_premium >= 2.00
  );

  // ASYMMETRIC_IVRAMP_FLAG — IV ramp already flagged + low rank + rising vols + uptrend
  const asymmetric_ivramp_flag = Boolean(
    row.iv_ramp_flag === true
    && ivRank != null && ivRank < 40
    && (row.iv_velocity_10d == null || row.iv_velocity_10d > 0)
    && (row.iv_velocity_20d == null || row.iv_velocity_20d > 0)
    && row.sma_golden_cross === true
    && row.sma_regime === "UPTREND"
    && spread != null && spread <= 0.15
  );

  const asymmetric_any_flag = asymmetric_cc_flag || asymmetric_csp_flag || asymmetric_ivramp_flag;

  const types = [];
  if (asymmetric_cc_flag)     types.push("CC");
  if (asymmetric_csp_flag)    types.push("CSP");
  if (asymmetric_ivramp_flag) types.push("IV_RAMP");

  const asymmetric_type =
    types.length === 3 ? "ALL_THREE" :
    types.length === 2 ? types.join("+") :
    types.length === 1 ? types[0] : null;

  return { asymmetric_cc_flag, asymmetric_csp_flag, asymmetric_ivramp_flag, asymmetric_any_flag, asymmetric_type };
}
